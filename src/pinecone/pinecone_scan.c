#include "pinecone_api.h"
#include "pinecone.h"

#include <storage/bufmgr.h>
#include "catalog/pg_operator_d.h"
#include "utils/rel.h"
#include "utils/builtins.h"


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
    so->collation = index->rd_indcollation[0]; // get the collation of the first attribute

    // create tuple description for sorting
    so->tupdesc = CreateTemplateTupleDesc(2);
    TupleDescInitEntry(so->tupdesc, (AttrNumber) 1, "distance", FLOAT8OID, -1, 0);
    TupleDescInitEntry(so->tupdesc, (AttrNumber) 2, "heaptid", TIDOID, -1, 0);

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
 * todo: can we reuse a connection created in pinecone_beginscan?
 */
void pinecone_rescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
	Vector * vec;
	cJSON *query_vector_values;
	// cJSON *pinecone_response;
    cJSON* fetch_ids;
    cJSON** responses;
    cJSON *query_response, *fetched_ids;
	cJSON *matches;
    Datum query_datum; // query vector
    PineconeStaticMetaPageData pinecone_metadata = PineconeSnapshotStaticMeta(scan->indexRelation);
    PineconeScanOpaque so = (PineconeScanOpaque) scan->opaque;
    TupleDesc tupdesc = RelationGetDescr(scan->indexRelation); // used for accessing
    cJSON* filter;

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
    // todo: we want to update T when we query the index.
    fetch_ids = get_fetch_ids(PineconeSnapshotBufferMeta(scan->indexRelation));
    responses = pinecone_query_with_fetch(pinecone_api_key, pinecone_metadata.host, pinecone_top_k, query_vector_values, filter, true, fetch_ids);
    query_response = responses[0];
    fetched_ids = responses[1];
    AdvanceLivenessTail(scan->indexRelation, fetched_ids);


    // copy pinecone_response to scan opaque
    // response has a matches array, set opaque to the child of matches aka first match
    matches = cJSON_GetObjectItemCaseSensitive(query_response, "matches");
    so->pinecone_results = matches->child;
    if (matches->child == NULL) {
        // todo: hint the user that the buffer might not be flushed
        ereport(NOTICE, (errcode(ERRCODE_NO_DATA),
                         errmsg("No matches found")));
    }

    /* Requires MVCC-compliant snapshot as not able to pin during sorting */
    /* https://www.postgresql.org/docs/current/index-locking.html */
    if (!IsMVCCSnapshot(scan->xs_snapshot))
        elog(ERROR, "non-MVCC snapshots are not supported with pinecone");

    // locally scan the buffer and add them to the sort state
    load_buffer_into_sort(scan->indexRelation, so, query_datum, tupdesc);

}

// todo: save stats from inserting from base table into the meta

void load_buffer_into_sort(Relation index, PineconeScanOpaque so, Datum query_datum, TupleDesc index_tupdesc)
{
    // todo: make sure that this is just as fast as pgvector's flatscan e.g. using vectorized operations
    TupleTableSlot *slot = MakeSingleTupleTableSlot(so->tupdesc, &TTSOpsVirtual);
    PineconeBufferMetaPageData buffer_meta = PineconeSnapshotBufferMeta(index);
    BlockNumber currentblkno = buffer_meta.pinecone_page; // todo: use pinecone_known_live_age (aka T)
    int n_sortedtuple = 0;

    // check H - P > max_local_scan
    if (buffer_meta.n_tuples - buffer_meta.n_pinecone_live_tuples > pinecone_max_buffer_scan) {
        int unflused = buffer_meta.n_tuples - buffer_meta.n_pinecone_tuples;
        int not_live = buffer_meta.n_pinecone_tuples - buffer_meta.n_pinecone_live_tuples;
        // warn the user that %d tuples have not yet been flushed and that %d tuples are not yet live in pinecone
        ereport(NOTICE, (errcode(ERRCODE_INSUFFICIENT_RESOURCES),
                         errmsg("Buffer is too large"),
                         errhint("There are %d tuples in the buffer that have not yet been flushed to pinecone and %d tuples in pinecone that are not yet live in pinecone. You may want to consider flushing the buffer.", unflused, not_live)));
    }

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
            IndexTuple itup;
            Datum datum;
            bool isnull;
            ItemId itemid = PageGetItemId(page, offno);

            itup = (IndexTuple) PageGetItem(page, itemid);
            datum = index_getattr(itup, 1, index_tupdesc, &isnull);
            if (isnull) elog(ERROR, "vector is null");

            // add the tuples
            ExecClearTuple(slot);
            slot->tts_values[0] = FunctionCall2Coll(so->procinfo, so->collation, datum, query_datum); // compute distance between entry and query
            slot->tts_isnull[0] = false;
            slot->tts_values[1] = PointerGetDatum(&itup->t_tid);
            slot->tts_isnull[1] = false;
            ExecStoreVirtualTuple(slot);

            elog(DEBUG1, "adding tuple to sortstate");
            tuplesort_puttupleslot(so->sortstate, slot);
            n_sortedtuple++;
            // log the number of tuples in the sortstate
            // elog(DEBUG1, "tuples in sortstate: %d", so->sortstate->memtupcount);
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
    double pinecone_best_dist;
    double buffer_best_dist;
    bool isnull;

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

    // merge the results from the buffer and the remote index
    if (match == NULL && !so->more_buffer_tuples) {
        return false;
    }
    else if (buffer_best_dist < pinecone_best_dist) {
        // use the buffer tuple
        Datum datum;
        datum = slot_getattr(so->slot, 2, &isnull);
        match_heaptid = *((ItemPointer) DatumGetPointer(datum));
        scan->xs_heaptid = match_heaptid;
        scan->xs_recheck = true;
        // get the next tuple from the sortstate
        so->more_buffer_tuples = tuplesort_gettupleslot(so->sortstate, true, false, so->slot, NULL);
    }
    else {
        // get the id of the match // interpret the id as a string
        id_str = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(match, "id"));
        match_heaptid = pinecone_id_get_heap_tid(id_str);
        scan->xs_heaptid = match_heaptid;
        // NEXT
        so->pinecone_results = so->pinecone_results->next;
    }
    scan->xs_recheckorderby = false;
    return true;
}

void no_endscan(IndexScanDesc scan) {};
