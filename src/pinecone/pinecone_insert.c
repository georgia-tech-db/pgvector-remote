#include "pinecone.h"

#include <access/generic_xlog.h>
#include <storage/bufmgr.h>
#include "utils/memutils.h"
#include "storage/lmgr.h"
#include "miscadmin.h" // MyDatabaseId

#include <access/heapam.h>

#define PINECONE_FLUSH_LOCK_IDENTIFIER 1969841813 // random number, uniquely identifies the pinecone insertion lock
#define PINECONE_APPEND_LOCK_IDENTIFIER 1969841814 // random number, uniquely identifies the pinecone append lock

#define SET_LOCKTAG_FLUSH(lock, index)  SET_LOCKTAG_ADVISORY(lock, MyDatabaseId, (uint32) index->rd_id, PINECONE_FLUSH_LOCK_IDENTIFIER, 0)
#define SET_LOCKTAG_APPEND(lock, index) SET_LOCKTAG_ADVISORY(lock, MyDatabaseId, (uint32) index->rd_id, PINECONE_APPEND_LOCK_IDENTIFIER, 0)

void PineconePageInit(Page page, Size pageSize)
{
    PineconeBufferOpaque opaque;
    PageInit(page, pageSize, sizeof(PineconeBufferOpaqueData));
    opaque = PineconePageGetOpaque(page);
    opaque->nextblkno = InvalidBlockNumber;
    opaque->prev_checkpoint_blkno = InvalidBlockNumber;
    opaque->checkpoint.is_checkpoint = false;
    // checkpoint
    // ItemPointerSetInvalid
}

/* 
 * add a tuple to the end of the buffer
 * return true if a new page was created
 */
bool AppendBufferTuple(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heapRel)
{
    // IndexTuple itup;
    GenericXLogState *state;
    Buffer buffer_meta_buf, insert_buf, newbuf = InvalidBuffer;
    Page buffer_meta_page, insert_page, newpage;
    PineconeBufferOpaque  new_opaque;
    PineconeBufferMetaPage buffer_meta;
    BlockNumber newblkno;
    LOCKTAG pinecone_append_lock;
    Size itemsz;
    PineconeBufferMetaPageData meta_snapshot;
    bool full;
    bool create_checkpoint = false;
    
    // prepare the index tuple
    // itup = index_form_tuple(RelationGetDescr(index), values, isnull);
    // itup->t_tid = *heap_tid;
    // itemsz = MAXALIGN(IndexTupleSize(itup));
    
    //
    PineconeBufferTuple buffer_tid;
    buffer_tid.tid = *heap_tid;
    itemsz = MAXALIGN(sizeof(PineconeBufferTuple));

    /* LOCKING STRATEGY FOR INSERTION
     * acquire append lock
     * read a snapshot of meta
     * acquire meta.insert_page
     * if insert_page is not full:
     *   add item to insert_page:
     * if insert_page is full:
     *   acquire meta
     *   acquire & create newpage
     *   add item to newpage:
     *     insert_page.nextblkno = newpage.blkno
     *     meta.n_unflushed_tuples += (tuples on old page)
     *     meta.insert_page = newpage.blkno
     *   if this qualifies as a checkpoint:
     *     newpage.prev_checkpoint = meta.latest_checkpoint
     *     meta.latest_checkpoint = newpage.blkno
     *     newpage.representative_vector_heap_tid = itup.t_tid
     *   release newpage, meta
     * release insert_page, newpage, meta
     * release append lock
     * (if it was full and threshold met, we will next try to advance pinecone head)
     */

    // acquire append lock
    SET_LOCKTAG_APPEND(pinecone_append_lock, index); LockAcquire(&pinecone_append_lock, ExclusiveLock, false, false);
    // start WAL logging
    state = GenericXLogStart(index);
    // read a snapshot of the buffer meta
    meta_snapshot = PineconeSnapshotBufferMeta(index);
    // acquire the insert page
    insert_buf = ReadBuffer(index, meta_snapshot.insert_page); LockBuffer(insert_buf, BUFFER_LOCK_EXCLUSIVE);
    insert_page = GenericXLogRegisterBuffer(state, insert_buf, 0);
    // check if the page is full and if we want to create a new checkpoint
    full = PageGetFreeSpace(insert_page) < itemsz;
    create_checkpoint = meta_snapshot.n_tuples_since_last_checkpoint + PageGetMaxOffsetNumber(insert_page) >= PINECONE_BATCH_SIZE;

    // add item to insert page
    if (!full && !create_checkpoint) {
        // PageAddItem(insert_page, (Item) itup, itemsz, InvalidOffsetNumber, false, false);
        PageAddItem(insert_page, (Item) &buffer_tid, itemsz, InvalidOffsetNumber, false, false);

        // log the number of items on this page MaxOffsetNumber
        elog(DEBUG1, "No new page! Page has %lu items", (unsigned long)PageGetMaxOffsetNumber(insert_page));

        // release insert_page
        GenericXLogFinish(state);
        UnlockReleaseBuffer(insert_buf);
    } else {
        // acquire the meta
        buffer_meta_buf = ReadBuffer(index, PINECONE_BUFFER_METAPAGE_BLKNO); LockBuffer(buffer_meta_buf, BUFFER_LOCK_EXCLUSIVE);
        buffer_meta_page = GenericXLogRegisterBuffer(state, buffer_meta_buf, 0);
        buffer_meta = PineconePageGetBufferMeta(buffer_meta_page);
        // acquire and create a new page
        LockRelationForExtension(index, ExclusiveLock); // acquire a lock to let us add pages to the relation (this isn't really necessary since we will always have the append lock anyway)
        newbuf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
        LockBuffer(newbuf, BUFFER_LOCK_EXCLUSIVE);
        UnlockRelationForExtension(index, ExclusiveLock);
        newpage = GenericXLogRegisterBuffer(state, newbuf, GENERIC_XLOG_FULL_IMAGE);
        PineconePageInit(newpage, BufferGetPageSize(newbuf));
        // check that there is room on the new page
        if (PageGetFreeSpace(newpage) < itemsz) elog(ERROR, "A new page was created, but it doesn't have enough space for the new tuple");
        // add item to new page
        // PageAddItem(newpage, (Item) itup, itemsz, InvalidOffsetNumber, false, false);
        PageAddItem(newpage, (Item) &buffer_tid, itemsz, InvalidOffsetNumber, false, false);
        // update insert_page nextblkno
        newblkno = BufferGetBlockNumber(newbuf);
        PineconePageGetOpaque(insert_page)->nextblkno = newblkno;
        // update meta
        buffer_meta->insert_page = newblkno;
        buffer_meta->n_tuples_since_last_checkpoint += PageGetMaxOffsetNumber(insert_page);
        // if this qualifies as a checkpoint, set this page as the latest head checkpoint
        if (create_checkpoint) {
            // create a checkpoint on the opaque of the new page
            new_opaque = PineconePageGetOpaque(newpage);
            new_opaque->prev_checkpoint_blkno = buffer_meta->latest_checkpoint.blkno;
            new_opaque->checkpoint = buffer_meta->latest_checkpoint;
            new_opaque->checkpoint.tid = *heap_tid; // we will assume we have inserted up to this point if we see this in pinecone
            new_opaque->checkpoint.blkno = newblkno;
            new_opaque->checkpoint.checkpoint_no += 1;
            new_opaque->checkpoint.n_preceding_tuples += buffer_meta->n_tuples_since_last_checkpoint;
            // set this page as the latest head checkpoint
            buffer_meta->latest_checkpoint = new_opaque->checkpoint;
            buffer_meta->n_tuples_since_last_checkpoint = 0;

        }
        // release insert_page, newpage, meta
        GenericXLogFinish(state);
        UnlockReleaseBuffer(insert_buf); UnlockReleaseBuffer(newbuf); UnlockReleaseBuffer(buffer_meta_buf);
    }
    // release append lock
    LockRelease(&pinecone_append_lock, ExclusiveLock, false);
    return create_checkpoint;
}

bool AppendBufferTupleInCtx(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heapRel, IndexUniqueCheck checkUnique, IndexInfo *indexInfo)
{
    MemoryContext oldCtx;
    MemoryContext insertCtx;
    bool checkpoint_created;
    // use a memory context because index_form_tuple can allocate 
    insertCtx = AllocSetContextCreate(CurrentMemoryContext,
                                      "Pinecone insert tuple temporary context",
                                      ALLOCSET_DEFAULT_SIZES);
    oldCtx = MemoryContextSwitchTo(insertCtx);
    checkpoint_created = AppendBufferTuple(index, values, isnull, heap_tid, heapRel);
    MemoryContextSwitchTo(oldCtx);
    MemoryContextDelete(insertCtx); // delete the temporary context
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
        elog(DEBUG1, "Checkpoint created. Flushing to Pinecone");
        FlushToPinecone(index);
        pinecone_print_relation(index);
    }

    // log the state of the relation for debugging

    return false;
}

// todo: it will make debugging a lot easier to have a way to pretty print the state of the relation e.g. how many tups per page


/*
 * Upload batches of vectors to pinecone.
 */
void FlushToPinecone(Relation index)
{
    Buffer buf, buffer_meta_buf;
    Page page, buffer_meta_page;
    BlockNumber currentblkno = PINECONE_BUFFER_HEAD_BLKNO;
    cJSON* json_vectors = cJSON_CreateArray();
    bool success;

    // take a snapshot of the buffer meta
    // we don't need to worry about another transaction advancing the pinecone tail because we have the pinecone insertion lock
    PineconeStaticMetaPageData static_meta = PineconeSnapshotStaticMeta(index);
    PineconeBufferMetaPageData buffer_meta = PineconeSnapshotBufferMeta(index);

    // acquire the pinecone insertion lock
    LOCKTAG pinecone_flush_lock;
    SET_LOCKTAG_FLUSH(pinecone_flush_lock, index);
    success = LockAcquire(&pinecone_flush_lock, ExclusiveLock, false, true);
    if (!success) {
        ereport(NOTICE, (errcode(ERRCODE_LOCK_NOT_AVAILABLE),
                        errmsg("Pinecone insertion lock not available"),
                        errhint("The pinecone insertion lock is currently held by another transaction. This is likely because the buffer is being advanced by another transaction. This is not an error, but it may cause a delay in the insertion of new vectors.")));
        return;
    }

    // get the first page
    buf = ReadBuffer(index, buffer_meta.flush_checkpoint.blkno);
    if (BufferIsInvalid(buf)) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                        errmsg("Pinecone buffer page not found")));
    }
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);

    // iterate through the pages
    while (true)
    {
        // IndexTuple itup = NULL;

        // Add all tuples on the page.
        for (int i = 1; i <= PageGetMaxOffsetNumber(page); i++)
        {
            cJSON* json_vector;
            PineconeBufferTuple buffer_tup = *((PineconeBufferTuple*) PageGetItem(page, PageGetItemId(page, i)));
            // log the tid of the index tuple
            elog(DEBUG1, "Flushing tuple with tid %d:%d", ItemPointerGetBlockNumber(&buffer_tup.tid), ItemPointerGetOffsetNumber(&buffer_tup.tid));

            // extern bool heap_fetch(Relation relation, Snapshot snapshot,
            //  HeapTuple tuple, Buffer *userbuf, bool keep_buf);
            // get a snapshot, fetch the tuple from the heap and print the datums
            {
                // get the base table
                Oid baseTableOid = index->rd_index->indrelid;
                Relation baseTableRel = RelationIdGetRelation(baseTableOid);


                // 
                Snapshot snapshot = GetActiveSnapshot();
                HeapTupleData heapTupData;
                Buffer heapBuf = InvalidBuffer;
                bool found;
                // ItemPointerSet(&(heapTupData.t_self), ItemPointerGetBlockNumber(&itup->t_tid), ItemPointerGetOffsetNumber(&itup->t_tid));
                heapTupData.t_self = buffer_tup.tid;
                found = heap_fetch(baseTableRel, snapshot, &heapTupData, &heapBuf, false);

                if (!found) {
                    ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                                    errmsg("Pinecone buffer page not found")));
                } else {
                    elog(NOTICE, "Fetched tuple from heap");
                    for (int k = 0; k < baseTableRel->rd_att->natts; k++) {
                        bool isnull;
                        FormData_pg_attribute attr = baseTableRel->rd_att->attrs[k];
                        Datum datum = heap_getattr(&heapTupData, k + 1, baseTableRel->rd_att, &isnull);
                        if (!isnull) {
                            int datum_str = DatumGetInt32(datum);
                            elog(NOTICE, "Attribute %s: %d", NameStr(attr.attname), datum_str);
                        }
                    }
                    elog(NOTICE, "That tuple had n attributes where n = %d", baseTableRel->rd_att->natts);
                    json_vector = heap_tuple_get_pinecone_vector(baseTableRel, &heapTupData);
                    elog(NOTICE, "Got the json vector: %s", cJSON_Print(json_vector));
                    cJSON_AddItemToArray(json_vectors, json_vector);
                }

                // release the buffer
                if (BufferIsValid(heapBuf)) {
                    ReleaseBuffer(heapBuf);
                }
                // todo: not quite so hideous
                RelationClose(baseTableRel);
            }
            // close the base table


            // json_vector = index_tuple_get_pinecone_vector(index, itup);
            // cJSON_AddItemToArray(json_vectors, json_vector);
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

        // If we have reached a checkpoint, push them to the remote index and update the pinecone checkpoint with a representative vector heap tid
        if (PineconePageGetOpaque(page)->checkpoint.is_checkpoint) {
            GenericXLogState *state = GenericXLogStart(index); // start a new WAL record
 
            pinecone_bulk_upsert(pinecone_api_key, static_meta.host, json_vectors, pinecone_vectors_per_request);

            // lock the buffer meta page
            buffer_meta_buf = ReadBuffer(index, PINECONE_BUFFER_METAPAGE_BLKNO);
            LockBuffer(buffer_meta_buf, BUFFER_LOCK_EXCLUSIVE);
            buffer_meta_page = GenericXLogRegisterBuffer(state, buffer_meta_buf, 0);

            // update the buffer meta page
            PineconePageGetBufferMeta(buffer_meta_page)->flush_checkpoint = PineconePageGetOpaque(page)->checkpoint;

            // save and release
            GenericXLogFinish(state);
            UnlockReleaseBuffer(buffer_meta_buf);

            // free
            cJSON_Delete(json_vectors); json_vectors = cJSON_CreateArray();

            // stop if we don't expect to have another batch because we have reached the last checkpoint
            if (buffer_meta.latest_checkpoint.blkno == currentblkno) break;
        }
    }
    UnlockReleaseBuffer(buf); // release the last buffer

    // release the lock
    LockRelease(&pinecone_flush_lock, ExclusiveLock, false);
}
