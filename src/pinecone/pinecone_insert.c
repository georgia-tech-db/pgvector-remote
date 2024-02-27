#include "pinecone.h"

#include <access/generic_xlog.h>
#include <storage/bufmgr.h>
#include "utils/memutils.h"
#include "storage/lmgr.h"
#include "miscadmin.h" // MyDatabaseId

bool AppendBufferTupleInCtx(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heapRel, IndexUniqueCheck checkUnique, IndexInfo *indexInfo)
{
    MemoryContext oldCtx;
    MemoryContext insertCtx;
    bool newpage;
    // use a memory context because index_form_tuple can allocate 
    insertCtx = AllocSetContextCreate(CurrentMemoryContext,
                                      "Pinecone insert tuple temporary context",
                                      ALLOCSET_DEFAULT_SIZES);
    oldCtx = MemoryContextSwitchTo(insertCtx);
    newpage = AppendBufferTuple(index, values, isnull, heap_tid, heapRel);
    MemoryContextSwitchTo(oldCtx);
    MemoryContextDelete(insertCtx); // delete the temporary context
    return newpage;
}

void PineconePageInit(Page page, Size pageSize)
{
    PageInit(page, pageSize, sizeof(PineconeBufferOpaqueData));
    PineconePageGetOpaque(page)->nextblkno = InvalidBlockNumber;
}

/* 
 * add a tuple to the end of the buffer
 * return true if a new page was created
 */
bool AppendBufferTuple(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heapRel)
{
    IndexTuple itup;
    GenericXLogState *state;
    Buffer buffer_meta_buf, insert_buf, newbuf = InvalidBuffer;
    Page buffer_meta_page, insert_page, newpage;
    PineconeBufferMetaPage buffer_meta;
    Size itemsz;
    bool success;
    bool full;
    bool checkpoint_created = false;
    
    // prepare the index tuple
    itup = index_form_tuple(RelationGetDescr(index), values, isnull);
    itup->t_tid = *heap_tid;
    itemsz = MAXALIGN(IndexTupleSize(itup));

    /* LOCKING STRATEGY FOR INSERTION
     * acquire meta
     * acquire insert_page
     * if insert_page is not full:
     *   add item to insert_page:
     * if insert_page is full:
     *   acquire & create new page
     *   add item to new page:
     *     update insert_page nextblkno
     *     update meta insert_page
     *   release newpage
     *   if this qualifies as a checkpoint, set it as the latest head checkpoint
     * update meta counts
     * release insert_page, meta
     * (if it was full and threshold met, we will next try to advance pinecone head)
     */

    // NEW
    // start WAL logging
    state = GenericXLogStart(index);
    // acquire the buffer metapage // todo: rename "buffer" something less confusing
    buffer_meta_buf = ReadBuffer(index, PINECONE_BUFFER_METAPAGE_BLKNO); LockBuffer(buffer_meta_buf, BUFFER_LOCK_EXCLUSIVE);
    buffer_meta_page = GenericXLogRegisterBuffer(state, buffer_meta_buf, 0); 
    buffer_meta = PineconePageGetBufferMeta(buffer_meta_page);
    // acquire the insert page
    insert_buf = ReadBuffer(index, buffer_meta->insert_page); LockBuffer(insert_buf, BUFFER_LOCK_EXCLUSIVE);
    insert_page = GenericXLogRegisterBuffer(state, insert_buf, 0);
    // check if the page is full
    full = PageGetFreeSpace(insert_page) < itemsz;
    if (full) {
        success = PageAddItem(insert_page, (Item) itup, itemsz, InvalidOffsetNumber, false, false);
    } else {
        // todo: we don't want to hold an exclusive lock on the buffer_meta page every time we want to insert a single tuple
        // we should only hold it when we need to update the meta because we are creating a new page
        // it isn't possible for another writer to interfere with us because we have a lock on the insert page
        // but another writer could see an outdated insert_page and try to acquire it.
        // acquire and create a new page
        LockRelationForExtension(index, ExclusiveLock); // acquire a lock to let us add pages to the relation (this isn't really necessary since we will always have the buffer_meta locked.)
        newbuf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
        LockBuffer(newbuf, BUFFER_LOCK_EXCLUSIVE);
        UnlockRelationForExtension(index, ExclusiveLock);
        newpage = GenericXLogRegisterBuffer(state, newbuf, GENERIC_XLOG_FULL_IMAGE);
        PineconePageInit(newpage, BufferGetPageSize(newbuf));
        // check that there is room on the new page
        if (PageGetFreeSpace(newpage) < itemsz) elog(ERROR, "A new page was created, but it doesn't have enough space for the new tuple");
        // add item to new page
        success = PageAddItem(newpage, (Item) itup, itemsz, InvalidOffsetNumber, false, false);
        // update insert_page nextblkno
        PineconePageGetOpaque(insert_page)->nextblkno = BufferGetBlockNumber(newbuf);
        // update meta insert_page
        buffer_meta->insert_page = BufferGetBlockNumber(newbuf);
        // if this qualifies as a checkpoint, set this page as the latest head checkpoint
        checkpoint_created = buffer_meta->n_tuples - buffer_meta->n_latest_head_checkpoint >= PINECONE_BATCH_SIZE;
        if (checkpoint_created) {
            buffer_meta->latest_head_checkpoint = buffer_meta->insert_page;
            buffer_meta->n_latest_head_checkpoint = buffer_meta->n_tuples;
        }
    }
    // update meta counts
    buffer_meta->n_tuples++;
    // release insert_page, meta
    GenericXLogFinish(state);
    UnlockReleaseBuffer(insert_buf);
    UnlockReleaseBuffer(buffer_meta_buf);
    if (success) {
        // todo: check page add item success
    }
    if (full) {
        UnlockReleaseBuffer(newbuf);
    }
    return checkpoint_created;
}


/*
 * Insert a tuple into the index
 */
bool pinecone_insert(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid,
                     Relation heap, IndexUniqueCheck checkUnique, 
#if PG_VERSION_NUM >= 140000
                     bool indexUnchanged, 
#endif
                     IndexInfo *indexInfo)
{
    bool checkpoint_created;

    // add a tuple to the buffer
    checkpoint_created = AppendBufferTupleInCtx(index, values, isnull, heap_tid, heap, checkUnique, indexInfo);

    // if there are enough tuples in the buffer, advance the pinecone tail
    if (checkpoint_created) {
        AdvancePineconeTail(index);
    }

    return false;
}

/*
 * Upload batches of vectors to pinecone.
 */
void AdvancePineconeTail(Relation index)
{
    Buffer buf;
    Page page;
    BlockNumber currentblkno = PINECONE_BUFFER_HEAD_BLKNO;
    cJSON* json_vectors = cJSON_CreateArray();
    bool success;

    // take a snapshot of the buffer meta
    // we don't need to worry about another transaction advancing the pinecone tail because we have the pinecone insertion lock
    PineconeStaticMetaPageData static_meta = PineconeSnapshotStaticMeta(index);
    PineconeBufferMetaPageData buffer_meta = PineconeSnapshotBufferMeta(index);

    // acquire the pinecone insertion lock
    LOCKTAG pinecone_insertion_lock;
    SET_LOCKTAG_ADVISORY(pinecone_insertion_lock, MyDatabaseId, (uint32) index->rd_id, PINECONE_INSERTION_LOCK_IDENTIFIER, 0);
    success = LockAcquire(&pinecone_insertion_lock, ExclusiveLock, false, true);
    if (!success) {
        ereport(NOTICE, (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                        errmsg("Pinecone insertion lock not available"),
                        errhint("The pinecone insertion lock is currently held by another transaction. This is likely because the buffer is being advanced.")));
        return;
    }

    // if the liveness tail is behind by more than PINECONE_N_CHECKPOINTS, abort
    if (buffer_meta.checkpoints[PINECONE_N_CHECKPOINTS - 1].page != InvalidBlockNumber) {
        ereport(NOTICE, (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("Pinecone head is too far ahead of the liveness tail"),
                        errhint("The buffer is %d vectors ahead of the liveness tail. More vectors will not be uploaded to pinecone until pinecone catches up on indexing.", buffer_meta.n_pinecone_tuples - buffer_meta.n_pinecone_live_tuples)));
        LockRelease(&pinecone_insertion_lock, ExclusiveLock, false);
        return;
    }

    // get the first page
    buf = ReadBuffer(index, buffer_meta.pinecone_page);
    if (BufferIsInvalid(buf)) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("Pinecone buffer page not found")));
    }
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);

    // iterate through the pages
    while (true)
    {
        IndexTuple itup = NULL;

        // Add all tuples on the page.
        for (int i = 1; i <= PageGetMaxOffsetNumber(page); i++)
        {
            cJSON* json_vector;
            itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, i));
            json_vector = index_tuple_get_pinecone_vector(index, itup);
            cJSON_AddItemToArray(json_vectors, json_vector);
        }

        // Move to the next page. Stop if there are no more pages.
        // todo: isn't an opaque unnecessary? Couldn't I just use nextblkno++ and check if it's valid?
        currentblkno = PineconePageGetOpaque(page)->nextblkno;
        if (!BlockNumberIsValid(currentblkno)) break; 

        // release the current buffer and get the next buffer
        UnlockReleaseBuffer(buf);
        buf = ReadBuffer(index, currentblkno);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);

        // If we have enough vectors, push them to the remote index and update the pinecone checkpoint
        if (cJSON_GetArraySize(json_vectors) >= PINECONE_BATCH_SIZE) {
            cJSON* fetched_ids;
            cJSON* fetch_ids;
            fetch_ids = get_fetch_ids(buffer_meta);
            fetched_ids = pinecone_bulk_upsert_with_fetch(pinecone_api_key, static_meta.host, json_vectors, pinecone_vectors_per_request, true, fetch_ids);

            // advance the liveness tail if any of the fetched_checkpoints are still checkpoints
            AdvanceLivenessTail(index, fetched_ids);

            // update the pinecone page and make a new checkpoint
            set_pinecone_page(index, currentblkno, cJSON_GetArraySize(json_vectors), itup->t_tid);
            // free
            cJSON_Delete(json_vectors); json_vectors = cJSON_CreateArray();
            // stop if we don't expect to have another batch
            if (currentblkno >= buffer_meta.latest_head_checkpoint) break;
        }
    }
    UnlockReleaseBuffer(buf); // release the last buffer

    // release the lock
    LockRelease(&pinecone_insertion_lock, ExclusiveLock, false);
}


cJSON* get_fetch_ids(PineconeBufferMetaPageData buffer_meta) {
    cJSON* fetch_ids = cJSON_CreateArray();
    // TODO
    // iter thru buffer_meta.checkpoints and push pinecone_id_from_heap_tid(representative_vector_tid) to fetch tids

    return fetch_ids;
}

void AdvanceLivenessTail(Relation index, cJSON* fetched_ids) {
    Page buffer_meta_page;
    PineconeBufferMetaPage buffer_meta;
    Buffer buffer_meta_buf = ReadBuffer(index, PINECONE_BUFFER_METAPAGE_BLKNO);
    GenericXLogState* state = GenericXLogStart(index);
    // get Buffer's MetaPage
    LockBuffer(buffer_meta_buf, BUFFER_LOCK_EXCLUSIVE);
    buffer_meta_page = GenericXLogRegisterBuffer(state, buffer_meta_buf, 0); 
    buffer_meta = PineconePageGetBufferMeta(buffer_meta_page);
    // search backwards through buffer_meta.checkpoints
    // if any of the checkpoint.representative_tid are in the fetched_ids
    // set this as the new pinecone_known_live_page and move back the list to make this the first checkpoint
    for (int i = PINECONE_N_CHECKPOINTS - 1; i >= 0; i--) {
        // TODO: this is an array not an object
        if (cJSON_IsString(cJSON_GetObjectItem(fetched_ids, pinecone_id_from_heap_tid(buffer_meta->checkpoints[i].representative_tid)))) {
            buffer_meta->pinecone_known_live_page = buffer_meta->checkpoints[i].page;
            // move back the list to make this the first checkpoint
            for (int j = i; j > 0; j--) {
                buffer_meta->checkpoints[j] = buffer_meta->checkpoints[j - 1];
            }
            buffer_meta->checkpoints[0].page = InvalidBlockNumber;
            break;
        }
    }
    // save
    UnlockReleaseBuffer(buffer_meta_buf);
    GenericXLogFinish(state);
}
