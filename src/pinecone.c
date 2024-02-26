#include "postgres.h"

#include "catalog/index.h"
#include "access/amapi.h"
#include "vector.h"
#include "pinecone_api.h"
#include "pinecone.h"
#include "cJSON.h"
#include <nodes/execnodes.h>
#include <nodes/pathnodes.h>
#include <utils/array.h>
#include "access/relscan.h"
#include <access/generic_xlog.h>
#include <storage/bufmgr.h>
#include "utils/guc.h"
#include "utils/builtins.h"
#include <access/reloptions.h>
#include <catalog/pg_attribute.h>
#include <unistd.h>
#include "utils/memutils.h"
#include "storage/lmgr.h"
#include "catalog/pg_operator_d.h"
#include "catalog/pg_type_d.h"
#include "lib/pairingheap.h"
#include "access/heapam.h"
#include "utils/rel.h"

#define PINECONE_METAPAGE_BLKNO 0
#define PINECONE_BUFFER_HEAD_BLKNO 1

#define PINECONE_DEFAULT_BATCH_SIZE 100

#if PG_VERSION_NUM < 150000
#define MarkGUCPrefixReserved(x) EmitWarningsOnPlaceholders(x)
#endif

const char* vector_metric_to_pinecone_metric[VECTOR_METRIC_COUNT] = {
    "",
    "euclidean",
    "cosine",
    "dotproduct"
};

typedef struct PineconeOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
    int         spec; // spec is a string; this is its offset in the rd_options
}			PineconeOptions;

char* pinecone_api_key = NULL;
int pinecone_top_k = 1000;
int pinecone_vectors_per_request = 100;
int pinecone_concurrent_requests = 20;
// todo: principled batch sizes. Do we ever want the buffer to be bigger than a multi-insert? Possibly if we want to let the buffer fill up when the remote index is down.
static relopt_kind pinecone_relopt_kind;


void
pinecone_spec_validator(const char *spec)
{
    bool empty = strcmp(spec, "") == 0;
    if (empty || cJSON_Parse(spec) == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                (empty ? errmsg("Spec cannot be empty") : errmsg("Invalid spec: %s", spec)),
                errhint("Spec should be a valid JSON object e.g. WITH (spec='{\"serverless\":{\"cloud\":\"aws\",\"region\":\"us-west-2\"}}').\n \
                        Refer to https://docs.pinecone.io/reference/create_index")));
    }
}

void
PineconeInit(void)
{
    pinecone_relopt_kind = add_reloption_kind();
    add_string_reloption(pinecone_relopt_kind, "spec",
                            "Specification of the Pinecone Index. Refer to https://docs.pinecone.io/reference/create_index",
                            "", 
                            pinecone_spec_validator,
                            AccessExclusiveLock);
    // todo: allow for specifying a hostname instead of asking to create it
    // todo: you can have a relopts_validator which validates the whole relopt set. This could be used to check that exactly one of spec or host is set
    DefineCustomStringVariable("pinecone.api_key", "Pinecone API key", "Pinecone API key",
                              &pinecone_api_key, "", 
                              PGC_SUSET, // restrict to superusers, takes immediate effect and is not saved in the configuration file 
                              0, NULL, NULL, NULL); // todo: you can have a check_hook that checks that the api key is valid.
    DefineCustomIntVariable("pinecone.top_k", "Pinecone top k", "Pinecone top k",
                            &pinecone_top_k,
                            10000, 1, 10000,
                            PGC_USERSET,
                            0, NULL, NULL, NULL);
    DefineCustomIntVariable("pinecone.vectors_per_request", "Pinecone vectors per request", "Pinecone vectors per request",
                            &pinecone_vectors_per_request,
                            100, 1, 1000,
                            PGC_USERSET,
                            0, NULL, NULL, NULL);
    DefineCustomIntVariable("pinecone.concurrent_requests", "Pinecone concurrent requests", "Pinecone concurrent requests",
                            &pinecone_concurrent_requests,
                            20, 1, 100,
                            PGC_USERSET,
                            0, NULL, NULL, NULL);
    MarkGUCPrefixReserved("pinecone");
}

VectorMetric
get_opclass_metric(Relation index)
{
    FmgrInfo *procinfo = index_getprocinfo(index, 1, 2); // lookup the second support function in the opclass for the first attribute
    Oid collation = index->rd_indcollation[0]; // get the collation of the first attribute
    Datum datum = FunctionCall0Coll(procinfo, collation); // call the support function
    return (VectorMetric) DatumGetInt32(datum);
}


typedef struct PineconeBuildState
{
    int64 indtuples; // total number of tuples indexed
    cJSON *json_vectors; // array of json vectors
    char host[100]; // the remote index's hostname
} PineconeBuildState;


static void
pinecone_build_callback(Relation index, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state)
{
    PineconeBuildState *buildstate = (PineconeBuildState *) state;
    TupleDesc itup_desc = index->rd_att;
    cJSON *json_vector;
    char* pinecone_id = pinecone_id_from_heap_tid(*tid);
    json_vector = tuple_get_pinecone_vector(itup_desc, values, isnull, pinecone_id);
    cJSON_AddItemToArray(buildstate->json_vectors, json_vector);
    if (cJSON_GetArraySize(buildstate->json_vectors) >= pinecone_vectors_per_request * pinecone_concurrent_requests) {
        pinecone_bulk_upsert(pinecone_api_key, buildstate->host, buildstate->json_vectors, pinecone_vectors_per_request);
        cJSON_Delete(buildstate->json_vectors);
        buildstate->json_vectors = cJSON_CreateArray();
    }
    buildstate->indtuples++;
}

void validate_api_key(void) {
    if (pinecone_api_key == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("Pinecone API key not set"),
                 errhint("Set the pinecone API key using the pinecone.api_key GUC. E.g. ALTER SYSTEM SET pinecone.api_key TO 'your-api-key'")));
    }
}

void generateRandomAlphanumeric(char *s, const int length) {
    char charset[] = "0123456789abcdefghijklmnopqrstuvwxyz";
    if (length) {
        // Seed the random number generator
        srand((unsigned int)time(NULL));
        for (int i = 0; i < length; i++) {
            int key = rand() % (int)(sizeof(charset) - 1);
            *s++ = charset[key];
        }
        *s = '\0'; // Null-terminate the string
    }
}


char* get_pinecone_index_name(Relation index) {
    char* pinecone_index_name = palloc(45); // pinecone's maximum index name length is 45
    char* index_name;
    char random_postfix[5];
    int name_length;
    // create the pinecone_index_name like pgvector-{oid}-{index_name}-{random_postfix}
    index_name = NameStr(index->rd_rel->relname);
    generateRandomAlphanumeric(random_postfix, 4);
    name_length = snprintf(pinecone_index_name, sizeof(pinecone_index_name), "pgvector-%u-%s-%s", index->rd_id, index_name, random_postfix);
    if (name_length >= sizeof(pinecone_index_name)) {
        ereport(ERROR, (errcode(ERRCODE_NAME_TOO_LONG), errmsg("Pinecone index name too long"), errhint("The pinecone index name is %s... and is %d characters long. The maximum length is 45 characters.", pinecone_index_name, name_length)));
    }
    // check that all chars are alphanumeric or hyphen
    for (int i = 0; i < name_length; i++) {
        if (!isalnum(pinecone_index_name[i]) && pinecone_index_name[i] != '-') {
            elog(DEBUG1, "Invalid character: %c", pinecone_index_name[i]);
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("Pinecone index name contains invalid characters"), errhint("The pinecone index name can only contain alphanumeric characters and hyphens.")));
        }
    }
    return pinecone_index_name;
}


IndexBuildResult *pinecone_build(Relation heap, Relation index, IndexInfo *indexInfo)
{
    PineconeOptions *opts = (PineconeOptions *) index->rd_options;
    IndexBuildResult *result = palloc(sizeof(IndexBuildResult));
    cJSON* spec_json = cJSON_Parse(GET_STRING_RELOPTION(opts, spec));
    char *host;

    validate_api_key();
    // create the remote index and get its hostname; also create the meta page
    host = CreatePineconeIndexAndWait(index, spec_json);
    // 
    CreateBufferHead(index, MAIN_FORKNUM);
    // iterate through the base table and upsert the vectors to the remote index
    BuildIndex(heap, index, indexInfo, host, result);
    return result;
}

char* CreatePineconeIndexAndWait(Relation index, cJSON* spec_json) {
    char* host = palloc(100);
    VectorMetric metric = get_opclass_metric(index);
    char* pinecone_metric_name = vector_metric_to_pinecone_metric[metric];
    char* pinecone_index_name = get_pinecone_index_name(index);
    int dimensions = TupleDescAttr(index->rd_att, 0)->atttypmod;
    cJSON* create_response = pinecone_create_index(pinecone_api_key, pinecone_index_name, dimensions, pinecone_metric_name, spec_json);
    host = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(create_response, "host"));
    // now we wait until the pinecone index is done initializing
    // todo: timeout
    while (true)
    {
        cJSON *describe_index_response;
        elog(DEBUG1, "Waiting for remote index to initialize...");
        sleep(1);
        describe_index_response = describe_index(pinecone_api_key, pinecone_index_name);
        if (cJSON_IsTrue(cJSON_GetObjectItem(cJSON_GetObjectItem(describe_index_response, "status"), "ready")))
        {
            break;
        }
    }
    // create the meta page
    CreateMetaPage(index, dimensions, host, pinecone_index_name,  metric, MAIN_FORKNUM);
    return host;
}

void BuildIndex(Relation heap, Relation index, IndexInfo *indexInfo, char* host, IndexBuildResult *result) {
    PineconeBuildState buildstate;
    int reltuples;
    // initialize the buildstate
    buildstate.indtuples = 0;
    buildstate.json_vectors = cJSON_CreateArray();
    strcpy(buildstate.host, host);
    // iterate through the base table and upsert the vectors to the remote index
    reltuples = table_index_build_scan(heap, index, indexInfo, true, true, pinecone_build_callback, (void *) &buildstate, NULL);
    if (cJSON_GetArraySize(buildstate.json_vectors) > 0) {
        pinecone_bulk_upsert(pinecone_api_key, host, buildstate.json_vectors, pinecone_vectors_per_request);
    }
    cJSON_Delete(buildstate.json_vectors);
    // stats
    result->heap_tuples = reltuples;
    result->index_tuples = buildstate.indtuples;
}

void no_buildempty(Relation index){}; // for some reason this is never called even when the base table is empty

#define PineconePageGetOpaque(page)	((PineconeBufferOpaque) PageGetSpecialPointer(page))
#define PineconePageGetMeta(page)	((PineconeMetaPageData *) PageGetContents(page))

void CreateMetaPage(Relation index, int dimensions, char *host, char *pinecone_index_name, int buffer_threshold, VectorMetric metric, int forkNum)
{
    Buffer buf;
    Page page; // a page is a block of memory, formatted as a page
    PineconeMetaPage metap;
    GenericXLogState *state;
    // create a new buffer
    buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE); // lock the buffer in exclusive mode meaning no other process can access it
    // register and initialize the page
    state = GenericXLogStart(index);
    page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
    PageInit(page, BufferGetPageSize(buf), 0); // third arg is the sizeof the page's opaque data
    metap = PineconePageGetMeta(page);
    metap->dimensions = dimensions;
    metap->buffer_fullness = 0;
    metap->buffer_threshold = buffer_threshold;
    metap->metric = metric;
    strcpy(metap->host, host);
    ((PageHeader) page)->pd_lower = ((char *) metap - (char *) page) + sizeof(PineconeMetaPageData);
    // cleanup
    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
}

// todo: this is too much boilerplate; I should just be able to get the page and release it
void incrMetaPageBufferFullness(Relation index)
{
    Buffer buf;
    Page page;
    PineconeMetaPage metap;
    GenericXLogState *state;
    buf = ReadBuffer(index, PINECONE_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    state = GenericXLogStart(index);
    page = GenericXLogRegisterBuffer(state, buf, 0);
    metap = PineconePageGetMeta(page);
    metap->buffer_fullness++;
    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
}

// todo: see above
void setMetaPageBufferFullnessZero(Relation index)
{
    Buffer buf;
    Page page;
    PineconeMetaPage metap;
    GenericXLogState *state;
    buf = ReadBuffer(index, PINECONE_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    state = GenericXLogStart(index);
    page = GenericXLogRegisterBuffer(state, buf, 0);
    metap = PineconePageGetMeta(page);
    metap->buffer_fullness = 0;
    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
}

void CreateBufferHead(Relation index, int forkNum)
{
    Buffer buf;
    Page page; // a page is a block of memory, formatted as a page
    GenericXLogState *state;
    // create a new buffer
    buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);
    LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE); // lock the buffer in exclusive mode meaning no other process can access it
    // register and initialize the page
    state = GenericXLogStart(index);
    page = GenericXLogRegisterBuffer(state, buf, GENERIC_XLOG_FULL_IMAGE);
    PageInit(page, BufferGetPageSize(buf), sizeof(PineconeBufferOpaqueData)); // third arg is the sizeof the page's opaque data
    // initialize the opaque data
    PineconePageGetOpaque(page)->nextblkno = InvalidBlockNumber;
    // cleanup
    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
}


// todo: you should return a pointer so that changes can take effect
PineconeMetaPageData ReadMetaPage(Relation index) {
    Buffer buf;
    Page page;
    PineconeMetaPage metap;
    buf = ReadBuffer(index, PINECONE_METAPAGE_BLKNO);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    metap = PineconePageGetMeta(page);
    UnlockReleaseBuffer(buf);
    return *metap;
}

void pinecone_buildempty(Relation index) {}

/*
 * Insert a tuple into the index
 */
bool pinecone_insert(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid,
                     Relation heap, IndexUniqueCheck checkUnique
#if PG_VERSION_NUM >= 140000
                     ,
                     bool indexUnchanged
#endif
                     ,
                     IndexInfo *indexInfo)
{
    PineconeMetaPageData pinecone_meta;
    cJSON *json_vectors;
    InsertBufferTupleMemCtx(index, values, isnull, heap_tid, heap, checkUnique, indexInfo);
    incrMetaPageBufferFullness(index);
    pinecone_meta = ReadMetaPage(index);
    // if the buffer is full, flush it to the remote index
    if (pinecone_meta.buffer_fullness == pinecone_meta.buffer_threshold) {
        elog(DEBUG1, "Buffer fullness = %d, flushing to remote index", pinecone_meta.buffer_fullness);
        json_vectors = get_buffer_pinecone_vectors(index);
        pinecone_bulk_upsert(pinecone_api_key, pinecone_meta.host, json_vectors, PINECONE_DEFAULT_BATCH_SIZE);
        elog(DEBUG1, "Buffer flushed to remote index. Now clearing buffer");
        clear_buffer(index);
        setMetaPageBufferFullnessZero(index);
    }
    return false;
}

cJSON* get_buffer_pinecone_vectors(Relation index)
{
    cJSON* json_vectors = cJSON_CreateArray();
    Buffer buf;
    Page page;
    BlockNumber currentblkno = PINECONE_BUFFER_HEAD_BLKNO;
    buf = ReadBuffer(index, currentblkno);
    LockBuffer(buf, BUFFER_LOCK_SHARE);
    page = BufferGetPage(buf);
    // iterate through the pages
    while (true)
    {
        // iterate through the tuples
        for (int i = 1; i <= PageGetMaxOffsetNumber(page); i++)
        {
            IndexTuple itup = (IndexTuple) PageGetItem(page, PageGetItemId(page, i));
            cJSON *json_vector = index_tuple_get_pinecone_vector(index, itup);
            cJSON_AddItemToArray(json_vectors, json_vector);
        }
        // get the next page
        currentblkno = PineconePageGetOpaque(page)->nextblkno;
        if (!BlockNumberIsValid(currentblkno)) break;
        // release the current buffer
        UnlockReleaseBuffer(buf);
        // get the next buffer
        buf = ReadBuffer(index, currentblkno);
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);
    }
    UnlockReleaseBuffer(buf);
    return json_vectors;
}

void check_vector_nonzero(Vector* vector) {
    if (vector_eq_zero_internal(vector)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid vector: zero vector"),
                        errhint("Pinecone insists that dense vectors cannot be zero in all dimensions. I don't know why they do this to you even when your metric isn't cosine.")));
    }
}

cJSON* tuple_get_pinecone_vector(TupleDesc tup_desc, Datum *values, bool *isnull, char *vector_id)
{
    cJSON *json_vector = cJSON_CreateObject();
    cJSON *metadata = cJSON_CreateObject();
    Vector *vector;
    cJSON *json_values;
    vector = DatumGetVector(values[0]);
    check_vector_nonzero(vector);
    json_values = cJSON_CreateFloatArray(vector->x, vector->dim);
    // prepare metadata
    for (int i = 0; i < tup_desc->natts; i++)
    {
        // todo: we should validate that all the columns have the desired types when the index is built
        FormData_pg_attribute* td = TupleDescAttr(tup_desc, i);
        if (td->atttypid == BOOLOID) {
            cJSON_AddItemToObject(metadata, NameStr(td->attname), cJSON_CreateBool(DatumGetBool(values[i])));
        } else if (td->atttypid == FLOAT8OID) {
            cJSON_AddItemToObject(metadata, NameStr(td->attname), cJSON_CreateNumber(DatumGetFloat8(values[i])));
        } else if (td->atttypid == TEXTOID) {
            cJSON_AddItemToObject(metadata, NameStr(td->attname), cJSON_CreateString(TextDatumGetCString(values[i])));
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


void InsertBufferTupleMemCtx(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heapRel, IndexUniqueCheck checkUnique, IndexInfo *indexInfo)
{
    MemoryContext oldCtx;
    MemoryContext insertCtx;
    // use a memory context because index_form_tuple can allocate 
    insertCtx = AllocSetContextCreate(CurrentMemoryContext,
                                      "Pinecone insert temporary context",
                                      ALLOCSET_DEFAULT_SIZES);
    oldCtx = MemoryContextSwitchTo(insertCtx);
    InsertBufferTuple(index, values, isnull, heap_tid, heapRel);
    MemoryContextSwitchTo(oldCtx);
    MemoryContextDelete(insertCtx); // delete the temporary context
}


void InsertBufferTuple(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heapRel)
{
    // OLD
    IndexTuple itup;
    BlockNumber insertPage;
    Size itemsz;
    Buffer buf;
    Page page;
    GenericXLogState *state;
    bool success;
    // NEW
    PineconeMetaPageData pinecone_meta;
    
    // NEW
    // create an index tuple
    // todo: factor this out
    itup = index_form_tuple(RelationGetDescr(index), values, isnull);
    itup->t_tid = *heap_tid;
    itemsz = MAXALIGN(IndexTupleSize(itup));

    // start WAL logging
    state = GenericXLogStart(index);

    // get the insert page
    pinecone_meta = ReadMetaPage(index);
    insertPage = pinecone_meta.insert_page;
    buf = ReadBuffer(index, insertPage); LockBuffer(buf, BUFFER_LOCK_EXCLUSIVE);
    page = BufferGetPage(buf); // todo understand snapshotting

    // if the page is full, create a new page
    // todo: factor this out by accepting a state
    if (PageGetFreeSpace(page) < itemsz) {
        // todo: acquire a lock on the meta page because we need to update the insert page and we don't want to let anyone else do it
        Buffer newbuf;
        Page newpage;
        // add a newpage
        LockRelationForExtension(index, ExclusiveLock); // acquire a lock to let us add pages to the relation
        newbuf = ReadBufferExtended(index, MAIN_FORKNUM, P_NEW, RBM_NORMAL, NULL);
        LockBuffer(newbuf, BUFFER_LOCK_EXCLUSIVE);
        UnlockRelationForExtension(index, ExclusiveLock);
        newpage = GenericXLogRegisterBuffer(state, newbuf, GENERIC_XLOG_FULL_IMAGE);
        PageInit(newpage, BufferGetPageSize(newbuf), sizeof(PineconeBufferOpaqueData));
        PineconePageGetOpaque(newpage)->nextblkno = InvalidBlockNumber; // todo put this and preceding line in a PineconePineInit function
        PineconePageGetOpaque(page)->nextblkno = BufferGetBlockNumber(newbuf);
        // set the insert page
        // PineconeSetMetaPage(index, BufferGetBlockNumber(newbuf));
        // todo: only increment the H, P, and T counts when they are assigned to a new page, which means we keep
        // track of how many tuples are on each page
        // we don't want each individual insertion to require getting a write lock on the meta page
        GenericXLogFinish(state);
        UnlockReleaseBuffer(buf);
        // prepare new buffer
        state = GenericXLogStart(index);
        buf = newbuf;
        page = GenericXLogRegisterBuffer(state, buf, 0);
    }

    // at this point we have a buffer with enough space for the new tuple
    if (PageGetFreeSpace(page) < itemsz) elog(ERROR, "A new page was created, but it doesn't have enough space for the new tuple");
    success = PageAddItem(page, (Item) itup, itemsz, InvalidOffsetNumber, false, false);
    if (!success) elog(ERROR, "failed to add item to page");
    GenericXLogFinish(state);
    UnlockReleaseBuffer(buf);
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



IndexBulkDeleteResult *pinecone_bulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
                                     IndexBulkDeleteCallback callback, void *callback_state)
{
    char* host = ReadMetaPage(info->index).host;
    cJSON* ids_to_delete = cJSON_CreateArray();
    // yikes! pinecone makes you read your ids sequentially in pages of 100, so vacuuming a large index is going to be impossible
    // todo: and list isn't even supported on pod-based indexes, so actually it would make sense simply to 
    // use /query to ask for 10K random ids and wait for them to be deleted.
    // but this doesn't quite work either since we need to iterate thru the whole thing.
    // the new plan of just keeping a-T is a lot less hacky. We care about their memory, not their disk space.
    cJSON* json_vectors = pinecone_list_vectors(pinecone_api_key, ReadMetaPage(info->index).host, 100, NULL);
    cJSON* json_vector; // todo: do I need to be freeing all my cJSON objects?
    elog(DEBUG1, "vacuuming json_vectors: %s", cJSON_Print(json_vectors));
    cJSON_ArrayForEach(json_vector, json_vectors) {
        char* id = cJSON_GetStringValue(cJSON_GetObjectItem(json_vector, "id"));
        ItemPointerData heap_tid = pinecone_id_get_heap_tid(id);
        if (callback(&heap_tid, callback_state)) cJSON_AddItemToArray(ids_to_delete, cJSON_CreateString(id));
    }
    elog(DEBUG1, "deleting ids: %s", cJSON_Print(ids_to_delete));
    pinecone_delete_vectors(pinecone_api_key, host, ids_to_delete);
    return NULL;
}

IndexBulkDeleteResult *no_vacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats) { return NULL; }

void
no_costestimate(PlannerInfo *root, IndexPath *path, double loop_count,
					Cost *indexStartupCost, Cost *indexTotalCost,
					Selectivity *indexSelectivity, double *indexCorrelation,
					double *indexPages)
{
    // todo: consider running a health check on the remote index and return infinity if it is not healthy
    if (list_length(path->indexorderbycols) == 0 || linitial_int(path->indexorderbycols) != 0) {
        elog(DEBUG1, "Index must be ordered by the first column");
        *indexTotalCost = 1000000;
        return;
    }
};


bytea * pinecone_options(Datum reloptions, bool validate)
{
    PineconeOptions *opts;
	static const relopt_parse_elt tab[] = {
		{"spec", RELOPT_TYPE_STRING, offsetof(PineconeOptions, spec)},
        {"buffer_threshold", RELOPT_TYPE_INT, offsetof(PineconeOptions, buffer_threshold)},
	};
    opts = (PineconeOptions *) build_reloptions(reloptions, validate,
                                      pinecone_relopt_kind,
                                      sizeof(PineconeOptions),
                                      tab, lengthof(tab));
	return (bytea *) opts;
}

bool no_validate(Oid opclassoid) { return true; }

/*
 * Prepare for an index scan
 */
IndexScanDesc
pinecone_beginscan(Relation index, int nkeys, int norderbys)
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
    // TODO allocate 10MB for the sort (we should actually need a lot less)
    so->sortstate = tuplesort_begin_heap(so->tupdesc, 1, attNums, sortOperators, sortCollations, nullsFirstFlags, pinecone_top_k, NULL, false);
    so->slot = MakeSingleTupleTableSlot(so->tupdesc, &TTSOpsMinimalTuple);
    //
    scan->opaque = so;
    return scan;
}

/*
 * Start or restart an index scan
 * todo: can we reuse a connection created in pinecone_beginscan?
 */
void
pinecone_rescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
	Vector * vec;
	cJSON *query_vector_values;
	cJSON *pinecone_response;
	cJSON *matches;
    Datum query_datum; // query vector
    PineconeMetaPageData pinecone_metadata;
    PineconeScanOpaque so = (PineconeScanOpaque) scan->opaque;
    BlockNumber currentblkno = PINECONE_BUFFER_HEAD_BLKNO;
    TupleTableSlot *slot = MakeSingleTupleTableSlot(so->tupdesc, &TTSOpsVirtual);
    TupleDesc tupdesc = RelationGetDescr(scan->indexRelation); // used for accessing
    // double tuples = 0;
    // filter
    const char* pinecone_filter_operators[] = {"$lt", "$lte", "$eq", "$gte", "$gt", "$ne"};
    cJSON *filter; // {"$and": [{"flag": {"$eq": true}}, {"price": {"$lt": 10}}]}  // example filter
    cJSON *and_list;
    // log the metadata
    elog(DEBUG1, "nkeys: %d", nkeys);
    pinecone_metadata = ReadMetaPage(scan->indexRelation);    
    so->dimensions = pinecone_metadata.dimensions;
    so->metric = pinecone_metadata.metric;

    // check that the ORDER BY is on the first column (which is assumed to be a column on vectors)
    if (scan->numberOfOrderBys == 0 || orderbys[0].sk_attno != 1) {
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("Index must be ordered by the first column")));
    }
    
    // todo: factor out building the filter
    // build the filter
    filter = cJSON_CreateObject();
    and_list = cJSON_CreateArray();
    // iterate thru the keys and build the filter
    for (int i = 0; i < nkeys; i++)
    {
        cJSON *key_filter = cJSON_CreateObject();
        cJSON *condition = cJSON_CreateObject();
        cJSON *condition_value;
        FormData_pg_attribute* td = TupleDescAttr(scan->indexRelation->rd_att, keys[i].sk_attno - 1);
        elog(DEBUG1, "tuple attr %d desc %s", keys[i].sk_attno, td->attname.data);
        if (td->atttypid == BOOLOID)
        {
            condition_value = cJSON_CreateBool(DatumGetBool(keys[i].sk_argument));
        } else if (td->atttypid == FLOAT8OID)
        {
            condition_value = cJSON_CreateNumber(DatumGetFloat8(keys[i].sk_argument));
        } else 
        {
            condition_value = cJSON_CreateString(TextDatumGetCString(keys[i].sk_argument));
        } 
        // this only works if all datatypes use the same strategy naming convention.
        cJSON_AddItemToObject(condition, pinecone_filter_operators[keys[i].sk_strategy - 1], condition_value);
        cJSON_AddItemToObject(key_filter, td->attname.data, condition);
        elog(DEBUG1, "key_filter: %s", cJSON_Print(condition));
        cJSON_AddItemToArray(and_list, key_filter);
    }
    cJSON_AddItemToObject(filter, "$and", and_list);
    elog(DEBUG1, "filter: %s", cJSON_Print(filter));

	// get the query vector
    query_datum = orderbys[0].sk_argument;
    vec = DatumGetVector(query_datum);
    query_vector_values = cJSON_CreateFloatArray(vec->x, vec->dim);
    pinecone_response = pinecone_api_query_index(pinecone_api_key, pinecone_metadata.host, 10000, query_vector_values, filter);
    elog(DEBUG1, "pinecone_response: %s", cJSON_Print(pinecone_response));
    // copy pinecone_response to scan opaque
    // response has a matches array, set opaque to the child of matches aka first match
    matches = cJSON_GetObjectItemCaseSensitive(pinecone_response, "matches");
    so->pinecone_results = matches->child;
    if (matches->child == NULL) {
        elog(NOTICE, "Pinecone did not return any results. This is expected in two cases: 1) pinecone needs a few seconds before the vectors are available for search 2) you have inserted fewer than pinecone.buffer_size = TODO vectors in which case all the vectors are still in the buffer and the buffer hasn't been flushed to the remote index yet. You can force a flush with TODO.");
    }
    
    // TODO understand these
    /* Count index scan for stats */
    // pgstat_count_index_scan(scan->indexRelation);

    /* Safety check */
    if (scan->orderByData == NULL)
        elog(ERROR, "cannot scan pinecone index without order");

    /* Requires MVCC-compliant snapshot as not able to pin during sorting */
    /* https://www.postgresql.org/docs/current/index-locking.html */
    if (!IsMVCCSnapshot(scan->xs_snapshot))
        elog(ERROR, "non-MVCC snapshots are not supported with pinecone");

    // todo: there may be postgres helpers for iterating thru the buffer and using a callback to add to the sortstate
    // todo: factor this out
    // ADD BUFFER TO THE SORT AND PERFORM THE SORT
    // TODO skip normlizaton for now
    // TODO create the sortstate
    while (BlockNumberIsValid(currentblkno)) {
        Buffer buf;
        Page page;
        Offset maxoffno;
        buf = ReadBuffer(scan->indexRelation, currentblkno); // todo bulkread access method
        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);
        maxoffno = PageGetMaxOffsetNumber(page);
        for (OffsetNumber offno = FirstOffsetNumber; offno <= maxoffno; offno = OffsetNumberNext(offno)) {
            IndexTuple itup;
            Datum datum;
            bool isnull;
            ItemId itemid = PageGetItemId(page, offno);

            itup = (IndexTuple) PageGetItem(page, itemid);
            datum = index_getattr(itup, 1, tupdesc, &isnull);
            if (isnull) elog(ERROR, "distance is null");


            // add the tuples
            ExecClearTuple(slot);
            slot->tts_values[0] = FunctionCall2Coll(so->procinfo, so->collation, datum, query_datum); // compute distance between entry and query
            slot->tts_isnull[0] = false;
            slot->tts_values[1] = PointerGetDatum(&itup->t_tid);
            slot->tts_isnull[1] = false;
            ExecStoreVirtualTuple(slot);

            elog(DEBUG1, "adding tuple to sortstate");
            tuplesort_puttupleslot(so->sortstate, slot);
            // log the number of tuples in the sortstate
            // elog(DEBUG1, "tuples in sortstate: %d", so->sortstate->memtupcount);
        }

        currentblkno = PineconePageGetOpaque(page)->nextblkno;
        UnlockReleaseBuffer(buf);
    }

    tuplesort_performsort(so->sortstate);
    
    // get the first tuple from the sortstate
    so->more_buffer_tuples = tuplesort_gettupleslot(so->sortstate, true, false, so->slot, NULL);

}

/*
 * Fetch the next tuple in the given scan
 */
bool
pinecone_gettuple(IndexScanDesc scan, ScanDirection dir)
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

/*
 * Define index handler
 *
 * See https://www.postgresql.org/docs/current/index-api.html
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(pineconehandler);
Datum pineconehandler(PG_FUNCTION_ARGS)
{
    IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

    amroutine->amstrategies = 0;
    amroutine->amsupport = 2; /* number of support functions */
#if PG_VERSION_NUM >= 130000
    amroutine->amoptsprocnum = 0;
#endif
    amroutine->amcanorder = false;
    amroutine->amcanorderbyop = true;
    amroutine->amcanbackward = false; /* can change direction mid-scan */
    amroutine->amcanunique = false;
    amroutine->amcanmulticol = true; /* TODO: pinecone can support filtered search */
    amroutine->amoptionalkey = true;
    amroutine->amsearcharray = false;
    amroutine->amsearchnulls = false;
    amroutine->amstorage = false;
    amroutine->amclusterable = false;
    amroutine->ampredlocks = false;
    amroutine->amcanparallel = false;
    amroutine->amcaninclude = false;
#if PG_VERSION_NUM >= 130000
    amroutine->amusemaintenanceworkmem = false; /* not used during VACUUM */
    amroutine->amparallelvacuumoptions = 0;
#endif
    amroutine->amkeytype = InvalidOid;

    /* Interface functions */
    amroutine->ambuild = pinecone_build;
    amroutine->ambuildempty = pinecone_buildempty;
    amroutine->aminsert = pinecone_insert;
    amroutine->ambulkdelete = pinecone_bulkdelete;
    amroutine->amvacuumcleanup = no_vacuumcleanup;
    // used to indicate if we support index-only scans; takes a attno and returns a bool;
    // included cols should always return true since there is little point in an included column if it can't be returned
    amroutine->amcanreturn = NULL; // do we support index-only scans?
    amroutine->amcostestimate = no_costestimate;
    amroutine->amoptions = pinecone_options;
    amroutine->amproperty = NULL;            /* TODO AMPROP_DISTANCE_ORDERABLE */
    amroutine->ambuildphasename = NULL;      // maps build phase number to name
    amroutine->amvalidate = no_validate; // check that the operator class is valid (provide the opclass's object id)
#if PG_VERSION_NUM >= 140000
    amroutine->amadjustmembers = NULL;
#endif
    amroutine->ambeginscan = pinecone_beginscan;
    amroutine->amrescan = pinecone_rescan;
    amroutine->amgettuple = pinecone_gettuple;
    amroutine->amgetbitmap = NULL; // an alternative to amgettuple that returns a bitmap of matching tuples
    amroutine->amendscan = no_endscan;
    amroutine->ammarkpos = NULL;
    amroutine->amrestrpos = NULL;

    /* Interface functions to support parallel index scans */
    amroutine->amestimateparallelscan = NULL;
    amroutine->aminitparallelscan = NULL;
    amroutine->amparallelrescan = NULL;

    PG_RETURN_POINTER(amroutine);
}
