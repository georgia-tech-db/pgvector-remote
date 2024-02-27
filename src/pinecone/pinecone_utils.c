#include "pinecone.h"

#include "storage/bufmgr.h"
#include "access/generic_xlog.h"
#include "access/relscan.h"
#include "utils/builtins.h"

cJSON* tuple_get_pinecone_vector(TupleDesc tup_desc, Datum *values, bool *isnull, char *vector_id)
{
    cJSON *json_vector = cJSON_CreateObject();
    cJSON *metadata = cJSON_CreateObject();
    Vector *vector;
    cJSON *json_values;
    vector = DatumGetVector(values[0]);
    validate_vector_nonzero(vector);
    json_values = cJSON_CreateFloatArray(vector->x, vector->dim);
    // prepare metadata
    for (int i = 1; i < tup_desc->natts; i++) // skip the first column which is the vector
    {
        // todo: we should validate that all the columns have the desired types when the index is built
        FormData_pg_attribute* td = TupleDescAttr(tup_desc, i);
        switch (td->atttypid) {
            case BOOLOID:
                cJSON_AddItemToObject(metadata, NameStr(td->attname), cJSON_CreateBool(DatumGetBool(values[i])));
                break;
            case FLOAT8OID:
                cJSON_AddItemToObject(metadata, NameStr(td->attname), cJSON_CreateNumber(DatumGetFloat8(values[i])));
                break;
            case TEXTOID:
                cJSON_AddItemToObject(metadata, NameStr(td->attname), cJSON_CreateString(text_to_cstring(DatumGetTextP(values[i]))));
                break;
            default:
                ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                                errmsg("Invalid column type when decoding tuple."),
                                errhint("Pinecone index only supports boolean, float8 and text columns")));
        }
    }
    // add to vector object
    cJSON_AddItemToObject(json_vector, "id", cJSON_CreateString(vector_id));
    cJSON_AddItemToObject(json_vector, "values", json_values);
    cJSON_AddItemToObject(json_vector, "metadata", metadata);
    return json_vector;
}

cJSON* index_tuple_get_pinecone_vector(Relation index, IndexTuple itup) {
    int natts = index->rd_att->natts;
    Datum *itup_values = (Datum *) palloc(sizeof(Datum) * natts);
    bool *itup_isnull = (bool *) palloc(sizeof(bool) * natts);
    TupleDesc itup_desc = index->rd_att;
    char* vector_id;
    index_deform_tuple(itup, itup_desc, itup_values, itup_isnull);
    vector_id = pinecone_id_from_heap_tid(itup->t_tid);
    return tuple_get_pinecone_vector(itup_desc, itup_values, itup_isnull, vector_id);
}

ItemPointerData pinecone_id_get_heap_tid(char *id)
{
    ItemPointerData heap_tid;
    int n_matched;
    n_matched = sscanf(id, "%04hx%04hx%04hx", &heap_tid.ip_blkid.bi_hi, &heap_tid.ip_blkid.bi_lo, &heap_tid.ip_posid);
    if (n_matched != 3) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid vector id"),
                        errhint("Vector id should be a 12-character hexadecimal string")));
    }
    return heap_tid;
}

char* pinecone_id_from_heap_tid(ItemPointerData heap_tid)
{
    char* id = palloc(12 + 1);
    snprintf(id, 12 + 1, "%04hx%04hx%04hx", heap_tid.ip_blkid.bi_hi, heap_tid.ip_blkid.bi_lo, heap_tid.ip_posid);
    return id;
}



PineconeStaticMetaPageData GetStaticMetaPageData(Relation index) {
    Buffer buf;
    Page page;
    PineconeStaticMetaPage metap;
    buf = ReadBuffer(index, PINECONE_STATIC_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    metap = PineconePageGetStaticMeta(page);
    UnlockReleaseBuffer(buf);
    return *metap;
}



PineconeStaticMetaPageData PineconeSnapshotStaticMeta(Relation index)
{
    Buffer buf;
    Page page;
    PineconeStaticMetaPage metap;
    buf = ReadBuffer(index, PINECONE_STATIC_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    metap = PineconePageGetStaticMeta(page);
    UnlockReleaseBuffer(buf);
    return *metap;
}

PineconeBufferMetaPageData PineconeSnapshotBufferMeta(Relation index)
{
    Buffer buf;
    Page page;
    PineconeBufferMetaPage metap;
    buf = ReadBuffer(index, PINECONE_BUFFER_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    metap = PineconePageGetBufferMeta(page);
    UnlockReleaseBuffer(buf);
    return *metap;
}
    

// todo: we don't really want to have specific functions for each modification
void set_pinecone_page(Relation index, BlockNumber page, int n_new_tuples, ItemPointerData representative_vector_heap_tid) {
    Page buffer_meta_page;
    PineconeBufferMetaPage buffer_meta;
    Buffer buffer_meta_buf = ReadBuffer(index, PINECONE_BUFFER_METAPAGE_BLKNO);
    GenericXLogState* state = GenericXLogStart(index);
    // get Buffer's MetaPage
    LockBuffer(buffer_meta_buf, BUFFER_LOCK_EXCLUSIVE);
    buffer_meta_page = GenericXLogRegisterBuffer(state, buffer_meta_buf, 0); 
    buffer_meta = PineconePageGetBufferMeta(buffer_meta_page);
    // update pinecone page and stats
    buffer_meta->n_pinecone_tuples += n_new_tuples;
    buffer_meta->pinecone_page = page;
    // add to checkpoints list
    for (int i = 0; i < PINECONE_N_CHECKPOINTS; i++) {
        if (buffer_meta->checkpoints[i].page == InvalidBlockNumber) {
            buffer_meta->checkpoints[i].page = page;
            buffer_meta->checkpoints[i].representative_tid = representative_vector_heap_tid;
            break;
        }
    }
    // save
    UnlockReleaseBuffer(buffer_meta_buf);
    GenericXLogFinish(state);
}
