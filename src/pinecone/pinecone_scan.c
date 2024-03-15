#include "pinecone_api.h"
#include "pinecone.h"

#include <storage/bufmgr.h>
#include "catalog/pg_operator_d.h"
#include "utils/rel.h"
#include "utils/builtins.h"
#include <time.h>
#include "common/hashfn.h"

#include <catalog/index.h>
#include <access/heapam.h>
#include <access/tableam.h>

#include <math.h>

PineconeCheckpoint* get_checkpoints_to_fetch(Relation index) {
    // starting at the current pinecone page, create a list of each checkpoint page's checkpoint (blkno, tid, checkpt_no)
    PineconeBufferMetaPageData buffer_meta = PineconeSnapshotBufferMeta(index);
    int n_checkpoints = buffer_meta.flush_checkpoint.checkpoint_no - buffer_meta.ready_checkpoint.checkpoint_no;
    PineconeCheckpoint* checkpoints;
    BlockNumber currentblkno = buffer_meta.flush_checkpoint.blkno;
    PineconeBufferOpaqueData opaque = PineconeSnapshotBufferOpaque(index, currentblkno);

    // don't fetch more than pinecone_max_fetched_vectors_for_liveness_check vectors
    if (n_checkpoints > pinecone_max_fetched_vectors_for_liveness_check) {
        elog(WARNING, "Pinecone's internal indexing is more than %d batches behind what you have send to pinecone (flushed). This means pinecone is not keeping up with the rate of insertion.", n_checkpoints);
        n_checkpoints = pinecone_max_fetched_vectors_for_liveness_check;
    }
    checkpoints = palloc((n_checkpoints+1) * sizeof(PineconeCheckpoint));

    // traverse from the flushed checkpoint back to the live checkpoint and append each checkpoint to the list
    for (int i = 0; i < n_checkpoints; i++) {
        // move to the previous checkpoint
        currentblkno = opaque.prev_checkpoint_blkno;
        opaque = PineconeSnapshotBufferOpaque(index, currentblkno);
        checkpoints[i] = opaque.checkpoint;
        // we don't want to fetch the checkpoint we are already at (this will be the last checkpoint in the list if we don't exceed the max_fetched_vectors_for_liveness_check limit)
        if (currentblkno == buffer_meta.ready_checkpoint.blkno) {
            checkpoints[i].is_checkpoint = false;
        }
    }
    // append a sentinel value
    checkpoints[n_checkpoints].is_checkpoint = false;
    return checkpoints;
}

cJSON* fetch_ids_from_checkpoints(PineconeCheckpoint* checkpoints) {
    cJSON* fetch_ids = cJSON_CreateArray();
    for (int i = 0; checkpoints[i].is_checkpoint; i++) {
        cJSON_AddItemToArray(fetch_ids, cJSON_CreateString(pinecone_id_from_heap_tid(checkpoints[i].tid)));
    }
    return fetch_ids;
}

PineconeCheckpoint get_best_fetched_checkpoint(Relation index, PineconeCheckpoint* checkpoints, cJSON* fetch_results) {
    // find the latest checkpoint that has was fetched (i.e. is in fetch_results)
    // todo: add timestamping so that we can assume that if the pinecone page is sufficiently old, we can assume it is live. (simple)

    // preprocess the results from a json object to a list of ItemPointerData
    PineconeCheckpoint best_checkpoint = {INVALID_CHECKPOINT_NUMBER, InvalidBlockNumber, {{0, 0},0}, 0};
    cJSON* vectors = cJSON_GetObjectItemCaseSensitive(fetch_results, "vectors");
    cJSON* vector;
    clock_t start, end;
    int n_fetched = cJSON_GetArraySize(vectors);
    ItemPointerData* fetched_tids = palloc(sizeof(ItemPointerData) * n_fetched);
    int k = 0;

    start = clock();
    cJSON_Print(vectors);
    end = clock();
    elog(DEBUG1, "time to print fetched vectors: %f", (double)(end - start) / CLOCKS_PER_SEC);
    cJSON_ArrayForEach(vector, vectors) {
        char* id_str = vector->string;
        fetched_tids[k++] = pinecone_id_get_heap_tid(id_str);
    }
    // log fetched tids
    for (int i = 0; i < n_fetched; i++) {
        elog(DEBUG1, "fetched tid: %s", pinecone_id_from_heap_tid(fetched_tids[i]));
    }

    // the checkpoints are listed in reverse chronological order, so we can return the first checkpoint that is in fetch_results
    for (int i = 0; checkpoints[i].is_checkpoint; i++) {
        // search for the checkpoint in the fetched tids
        for (int j = 0; j < n_fetched; j++) {
            if (ItemPointerEquals(&checkpoints[i].tid, &fetched_tids[j])) {
                return checkpoints[i];
            }
        }
    }
    return best_checkpoint;
}

/*
 * Prepare for an index scan
 */
IndexScanDesc pinecone_beginscan(Relation index, int nkeys, int norderbys)
{
	IndexScanDesc scan;
    PineconeScanOpaque so;
    AttrNumber attNums[] = {1}; // sort only on the first column
	Oid			sortOperators[] = {Float8LessOperator};
	Oid			sortCollations[] = {InvalidOid};
	bool		nullsFirstFlags[] = {false};
	scan = RelationGetIndexScan(index, nkeys, norderbys);
    so = (PineconeScanOpaque) palloc(sizeof(PineconeScanOpaqueData));

    // set support functions
    so->procinfo = index_getprocinfo(index, 1, 1); // lookup the first support function in the opclass for the first attribute

    // create tuple description for sorting
    so->tupdesc = CreateTemplateTupleDesc(3);
    TupleDescInitEntry(so->tupdesc, (AttrNumber) 1, "distance", FLOAT8OID, -1, 0);
    TupleDescInitEntry(so->tupdesc, (AttrNumber) 2, "heaptid_blkno", INT4OID, -1, 0);
    TupleDescInitEntry(so->tupdesc, (AttrNumber) 3, "heaptid_offset", INT2OID, -1, 0);

    // prep sort
    // allocate 6MB for the heapsort
    so->sortstate = tuplesort_begin_heap(so->tupdesc, 1, attNums, sortOperators, sortCollations, nullsFirstFlags, 6000, NULL, false);
    so->slot = MakeSingleTupleTableSlot(so->tupdesc, &TTSOpsMinimalTuple);
    
    scan->opaque = so;
    return scan;
}


cJSON* pinecone_build_filter(Relation index, ScanKey keys, int nkeys) {
    cJSON *filter = cJSON_CreateObject();
    cJSON *and_list = cJSON_CreateArray();
    const char* pinecone_filter_operators[] = {"$lt", "$lte", "$eq", "$gte", "$gt", "$ne"};
    for (int i = 0; i < nkeys; i++)
    {
        cJSON *key_filter = cJSON_CreateObject();
        cJSON *condition = cJSON_CreateObject();
        cJSON *condition_value = NULL;
        FormData_pg_attribute* td = TupleDescAttr(index->rd_att, keys[i].sk_attno - 1);

        switch (td->atttypid)
        {
            case BOOLOID:
                condition_value = cJSON_CreateBool(DatumGetBool(keys[i].sk_argument));
                break;
            case FLOAT8OID:
                condition_value = cJSON_CreateNumber(DatumGetFloat8(keys[i].sk_argument));
                break;
            case TEXTOID:
                condition_value = cJSON_CreateString(text_to_cstring(DatumGetTextP(keys[i].sk_argument)));
                break;
            default:
                continue; // skip unsupported types
        }
        
        // this only works if all datatypes use the same strategy naming convention. todo: document this
        cJSON_AddItemToObject(condition, pinecone_filter_operators[keys[i].sk_strategy - 1], condition_value);
        cJSON_AddItemToObject(key_filter, td->attname.data, condition);
        cJSON_AddItemToArray(and_list, key_filter);
    }
    cJSON_AddItemToObject(filter, "$and", and_list);
    return filter;
}


/*
 * Start or restart an index scan
 * todo: can we reuse a tcp connection created in pinecone_beginscan?
 */
void pinecone_rescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
	Vector * vec;
	cJSON *query_vector_values;
	// cJSON *pinecone_response;
    cJSON* fetch_ids;
    PineconeCheckpoint* fetch_checkpoints;
    cJSON** responses;
    cJSON *query_response, *fetch_response;
	cJSON *matches;
    Datum query_datum; // query vector
    PineconeStaticMetaPageData pinecone_metadata = PineconeSnapshotStaticMeta(scan->indexRelation);
    PineconeScanOpaque so = (PineconeScanOpaque) scan->opaque;
    TupleDesc tupdesc = RelationGetDescr(scan->indexRelation); // used for accessing
    cJSON* filter;
    PineconeCheckpoint best_checkpoint;

    // check that the ORDER BY is on the first column (which is assumed to be a column on vectors)
    if (scan->numberOfOrderBys == 0 || orderbys[0].sk_attno != 1) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("Index must be ordered by the first column")));
    }
    
    // build the filter
    filter = pinecone_build_filter(scan->indexRelation, keys, nkeys);
    elog(DEBUG1, "filter: %s", cJSON_Print(filter));

	// get the query vector
    query_datum = orderbys[0].sk_argument;
    vec = DatumGetVector(query_datum);
    query_vector_values = cJSON_CreateFloatArray(vec->x, vec->dim);

    // query pinecone top-k
    fetch_checkpoints = get_checkpoints_to_fetch(scan->indexRelation);
    fetch_ids = fetch_ids_from_checkpoints(fetch_checkpoints);
    responses = pinecone_query_with_fetch(pinecone_api_key, pinecone_metadata.host, pinecone_top_k, query_vector_values, filter, true, fetch_ids);
    query_response = responses[0];
    fetch_response = responses[1];
    elog(DEBUG1, "query_response: %s", cJSON_Print(query_response));
    elog(DEBUG1, "fetch_response: %s", cJSON_Print(fetch_response));
    best_checkpoint = get_best_fetched_checkpoint(scan->indexRelation, fetch_checkpoints, fetch_response);

    // set the pinecone_ready_page to the best checkpoint
    if (best_checkpoint.is_checkpoint) {
        set_buffer_meta_page(scan->indexRelation, &best_checkpoint, NULL, NULL, NULL, NULL);
    }

    // copy metric
    so->metric = pinecone_metadata.metric;

    // copy pinecone_response to scan opaque
    // response has a matches array, set opaque to the child of matches aka first match
    matches = cJSON_GetObjectItemCaseSensitive(query_response, "matches");
    so->pinecone_results = matches->child;
    if (matches->child == NULL) {
        // todo: hint the user that the buffer might not be flushed
        ereport(DEBUG1, (errcode(ERRCODE_NO_DATA),
                         errmsg("No matches found")));
    }

    /* Requires MVCC-compliant snapshot as not able to pin during sorting */
    /* https://www.postgresql.org/docs/current/index-locking.html */
    if (!IsMVCCSnapshot(scan->xs_snapshot))
        elog(ERROR, "non-MVCC snapshots are not supported with pinecone");

    // locally scan the buffer and add them to the sort state
    load_buffer_into_sort(scan->indexRelation, so, query_datum, tupdesc);
    
    // allocate for xs_orderbyvals (*Datum)
    scan->xs_orderbyvals = palloc(sizeof(Datum)); // assumes only one ORDER BY
    scan->xs_orderbynulls = palloc(sizeof(bool)); // TODO: assumes only one ORDER BY

}

// todo: save stats from inserting from base table into the meta

void load_buffer_into_sort(Relation index, PineconeScanOpaque so, Datum query_datum, TupleDesc index_tupdesc)
{
    // todo: make sure that this is just as fast as pgvector's flatscan e.g. using vectorized operations
    TupleTableSlot *slot = MakeSingleTupleTableSlot(so->tupdesc, &TTSOpsVirtual);
    PineconeBufferMetaPageData buffer_meta = PineconeSnapshotBufferMeta(index);
    BlockNumber currentblkno = buffer_meta.ready_checkpoint.blkno;
    int n_sortedtuple = 0;
    int n_tuples = buffer_meta.latest_checkpoint.n_preceding_tuples + buffer_meta.n_tuples_since_last_checkpoint;
    int unflushed_tuples = n_tuples - buffer_meta.flush_checkpoint.n_preceding_tuples;
    int unready_tuples = n_tuples - buffer_meta.ready_checkpoint.n_preceding_tuples;
    size_t bloom_filter_size = (((int) (1.44 * BUFFER_BLOOM_K * unready_tuples))>>3) + 1; // bloom filter size in bytes, 1.44 is the optimal bloom filter expansion factor

    // index info
    IndexInfo *indexInfo = BuildIndexInfo(index);
    Datum* index_values = palloc(sizeof(Datum) * indexInfo->ii_NumIndexAttrs);
    bool* index_isnull = palloc(sizeof(bool) * indexInfo->ii_NumIndexAttrs);
    // get the base table
    Oid baseTableOid = index->rd_index->indrelid;
    Relation baseTableRel = RelationIdGetRelation(baseTableOid);
    Snapshot snapshot = GetActiveSnapshot();
    // begin the index fetch (this the preferred way for an index to request tuples from its base table)
    IndexFetchTableData *fetchData = baseTableRel->rd_tableam->index_fetch_begin(baseTableRel);
    TupleTableSlot *base_table_slot = MakeSingleTupleTableSlot(baseTableRel->rd_att, &TTSOpsBufferHeapTuple);
    bool call_again, all_dead, found;
    
    // check H - T > max_local_scan
    if (unready_tuples > pinecone_max_buffer_scan) {
        ereport(NOTICE, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                         errmsg("Buffer is too large"),
                         errhint("There are %d tuples in the buffer that have not yet been flushed to pinecone and %d tuples in pinecone that are not yet live. You may want to consider flushing the buffer.", unflushed_tuples, unready_tuples - unflushed_tuples)));
    }

    // initialize the bloom filter
    // so->bloom_filter = bloom_create(BUFFER_BLOOM_K, buffer_meta.n_tuples_since_last_checkpoint);
    so->bloom_filter = palloc0(bloom_filter_size);
    so->bloom_filter_size = bloom_filter_size;


    // add tuples to the sortstate
    while (BlockNumberIsValid(currentblkno)) {
        Buffer buf;
        Page page;


        // access the page
        buf = ReadBuffer(index, currentblkno); // todo bulkread access method
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);

        // add all tuples on the page to the sortstate
        for (OffsetNumber offno = FirstOffsetNumber; offno <= PageGetMaxOffsetNumber(page); offno = OffsetNumberNext(offno)) {
            // get the tid and the vector from the heap tuple
            ItemId itemid;
            Item item;
            PineconeBufferTuple buffer_tup;
            itemid = PageGetItemId(page, offno);
            item = PageGetItem(page, itemid);
            buffer_tup = *((PineconeBufferTuple*) item);
 
            // add the tuple to the bloom filter
            for (int i = 0; i < BUFFER_BLOOM_K; i++) {
                uint32 hash = hash_tid(buffer_tup.tid, i); // i is the seed
                so->bloom_filter[(hash >> 3) % so->bloom_filter_size] |= (1 << (hash & 7));
            }

            // fetch the vector from the base table
            found = baseTableRel->rd_tableam->index_fetch_tuple(fetchData, &buffer_tup.tid, snapshot, base_table_slot, &call_again, &all_dead);
            if (!found) {
                elog(DEBUG2, "could not find tuple in base table");
                elog(DEBUG2, "call_again: %d, all_dead: %d", call_again, all_dead);
                continue; // do not add the tuple to the sortstate
            }

            // extract the indexed columns
            FormIndexDatum(indexInfo, base_table_slot, NULL, index_values, index_isnull);

            if (index_isnull[0]) elog(ERROR, "vector is null");
           
            // add the tuples
            ExecClearTuple(slot);
            slot->tts_values[0] = FunctionCall2(so->procinfo, index_values[0], query_datum); // compute distance between entry and query
            slot->tts_isnull[0] = false;
            slot->tts_values[1] = Int32GetDatum(ItemPointerGetBlockNumber(&buffer_tup.tid));
            slot->tts_isnull[1] = false;
            slot->tts_values[2] = Int16GetDatum(ItemPointerGetOffsetNumber(&buffer_tup.tid));
            slot->tts_isnull[2] = false;
            ExecStoreVirtualTuple(slot);

            tuplesort_puttupleslot(so->sortstate, slot);
            n_sortedtuple++;
        }

        // move to the next page
        currentblkno = PineconePageGetOpaque(page)->nextblkno;
        UnlockReleaseBuffer(buf);

        // stop if we have added enough tuples to the sortstate
        if (n_sortedtuple >= pinecone_max_buffer_scan) {
            elog(NOTICE, "Reached max local scan");
            break;
        }
    }
    // end the index fetch
    ExecDropSingleTupleTableSlot(base_table_slot);
    baseTableRel->rd_tableam->index_fetch_end(fetchData);
    // close the base table
    RelationClose(baseTableRel);

    tuplesort_performsort(so->sortstate);
    
    // get the first tuple from the sortstate
    so->more_buffer_tuples = tuplesort_gettupleslot(so->sortstate, true, false, so->slot, NULL);
}

/*
 * Fetch the next tuple in the given scan
 */
bool pinecone_gettuple(IndexScanDesc scan, ScanDirection dir)
{
	// interpret scan->opaque as a cJSON object
	char *id_str;
	ItemPointerData match_heaptid;
    PineconeScanOpaque so = (PineconeScanOpaque) scan->opaque;
    cJSON *match = so->pinecone_results;
    double pinecone_best_dist, buffer_best_dist, dist, dist_lower_bound;
    bool isnull;
    float rel_tol = 0.05; // relative tolerance for distance recheck; TODO: this should depend on the metric; the inaccuracy arises from pinecone using half precision floats

    // while the match is in the bloom filter, get the next match
    while (match != NULL) {
        bool duplicate = true;
        id_str = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(match, "id"));
        match_heaptid = pinecone_id_get_heap_tid(id_str);
        for (int i = 0; i < BUFFER_BLOOM_K; i++) {
            uint32 hash = hash_tid(match_heaptid, i); // i is the seed
            if (!(so->bloom_filter[(hash >> 3) % so->bloom_filter_size] & (1 << (hash & 7)))) {
                duplicate = false;
                break;
            }
        }
        if (duplicate) {
            elog(DEBUG1, "skipping duplicate match %s. this was returned by pinecone, but was also found in the local buffer", id_str);
            match = match->next;
            so->pinecone_results = match;
        } else {
            break;
        }
    }

    // use a case statement to determine the best distance
    if (match == NULL) {
        pinecone_best_dist = __DBL_MAX__;
    } else {
        switch (so->metric)
        {
        case EUCLIDEAN_METRIC:
            // pinecone returns the square of the euclidean distance, which is what we want
            pinecone_best_dist = cJSON_GetNumberValue(cJSON_GetObjectItemCaseSensitive(match, "score"));
            break;
        case COSINE_METRIC:
            // pinecone returns the cosine similarity, but we want "cosine distance" which is 1 - cosine similarity
            pinecone_best_dist = 1 - cJSON_GetNumberValue(cJSON_GetObjectItemCaseSensitive(match, "score"));
            break;
        case INNER_PRODUCT_METRIC:
            // pinecone returns the dot product, but we want "dot product distance" which is - dot product
            pinecone_best_dist = - cJSON_GetNumberValue(cJSON_GetObjectItemCaseSensitive(match, "score"));
            break;
        default:
            elog(ERROR, "unsupported metric");
        }
    }
                          
    buffer_best_dist = (so->more_buffer_tuples) ? DatumGetFloat8(slot_getattr(so->slot, 1, &isnull)) : __DBL_MAX__;
    // log (match == NULL) so->more_buffer_tuples and the scores

    elog(DEBUG1, "✓ pinecone_best_dist: %f, buffer_best_dist: %f", pinecone_best_dist, buffer_best_dist);
    // merge the results from the buffer and the remote index
    if (match == NULL && !so->more_buffer_tuples) {
        return false;
    }
    else if (buffer_best_dist < pinecone_best_dist) {
        // use the buffer tuple
        Datum blkno_datum = slot_getattr(so->slot, 2, &isnull);
        Datum offset_datum = slot_getattr(so->slot, 3, &isnull);
        dist = buffer_best_dist;
        ItemPointerSetBlockNumber(&match_heaptid, blkno_datum);
        ItemPointerSetOffsetNumber(&match_heaptid, offset_datum);
        scan->xs_heaptid = match_heaptid;
        scan->xs_recheck = true;
        // get the next tuple from the sortstate
        so->more_buffer_tuples = tuplesort_gettupleslot(so->sortstate, true, false, so->slot, NULL);
    }
    else {
        dist = pinecone_best_dist;
        // get the id of the match // interpret the id as a string
        id_str = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(match, "id"));
        match_heaptid = pinecone_id_get_heap_tid(id_str);
        scan->xs_heaptid = match_heaptid;
        // TODO: create a datum out of the distance and retrun it to xs_orderbyvals
        // NEXT
        so->pinecone_results = so->pinecone_results->next;
    }
    // The recheck is going to compute vector<->query i.e. l2_distance, whereas for sorting we have been using l2_squared_distance
    // we need to provide xs_recheck a lower bound on the l2_distance
    dist_lower_bound = dist > 0 ? dist * (1 - rel_tol) : dist * (1 + rel_tol);
    dist_lower_bound = sqrt(dist_lower_bound);
    scan->xs_recheckorderby = true; // pinecone returns an approximate distance which we need to recheck.
    scan->xs_orderbyvals[0] = Float8GetDatum((float8) dist_lower_bound);
    scan->xs_orderbynulls[0] = false;
    elog(DEBUG1, "dist: %f, dist_lower_bound: %f", dist, dist_lower_bound);
    return true;
}

void no_endscan(IndexScanDesc scan) {};
