#include "pinecone_api.h"
#include "pinecone.h"

#include "postgres.h"

#include "catalog/index.h"
#include "access/amapi.h"
#include "src/vector.h"
#include "src/cJSON.h"
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
#include "miscadmin.h" // MyDatabaseId


#if PG_VERSION_NUM < 150000
#define MarkGUCPrefixReserved(x) EmitWarningsOnPlaceholders(x)
#endif

const char* vector_metric_to_pinecone_metric[VECTOR_METRIC_COUNT] = {
    "",
    "euclidean",
    "cosine",
    "dotproduct"
};

char* pinecone_api_key = NULL;
int pinecone_top_k = 1000;
int pinecone_vectors_per_request = 100;
int pinecone_concurrent_requests = 20;
int pinecone_max_buffer_scan = 10000; // maximum number of tuples to search in the buffer

// todo: principled batch sizes. Do we ever want the buffer to be bigger than a multi-insert? Possibly if we want to let the buffer fill up when the remote index is down.
static relopt_kind pinecone_relopt_kind;

void pinecone_spec_validator(const char *spec)
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

void PineconeInit(void)
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
    DefineCustomIntVariable("pinecone.max_buffer_search", "Pinecone max buffer search", "Pinecone max buffer search",
                            &pinecone_max_buffer_scan,
                            10000, 1, 100000,
                            PGC_USERSET,
                            0, NULL, NULL, NULL);
    MarkGUCPrefixReserved("pinecone");
}

VectorMetric get_opclass_metric(Relation index)
{
    FmgrInfo *procinfo = index_getprocinfo(index, 1, 2); // lookup the second support function in the opclass for the first attribute
    Oid collation = index->rd_indcollation[0]; // get the collation of the first attribute
    Datum datum = FunctionCall0Coll(procinfo, collation); // call the support function
    return (VectorMetric) DatumGetInt32(datum);
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
    VectorMetric metric = get_opclass_metric(index);
    cJSON* spec_json = cJSON_Parse(GET_STRING_RELOPTION(opts, spec));
    int dimensions = TupleDescAttr(index->rd_att, 0)->atttypmod;
    char* pinecone_index_name = get_pinecone_index_name(index);
    char* host;

    validate_api_key();
    // create the remote index and get its hostname
    host = CreatePineconeIndexAndWait(index, spec_json, metric, pinecone_index_name, dimensions);
    // init the index pages: static meta, buffer meta, and buffer head
    InitIndexPages(index, metric, dimensions, pinecone_index_name, host, MAIN_FORKNUM);
    // iterate through the base table and upsert the vectors to the remote index
    InsertBaseTable(heap, index, indexInfo, host, result);
    return result;
}

char* CreatePineconeIndexAndWait(Relation index, cJSON* spec_json, VectorMetric metric, char* pinecone_index_name, int dimensions) {
    char* host = palloc(100);
    const char* pinecone_metric_name = vector_metric_to_pinecone_metric[metric];
    cJSON* create_response = pinecone_create_index(pinecone_api_key, pinecone_index_name, dimensions, pinecone_metric_name, spec_json);
    host = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(create_response, "host"));
    // now we wait until the pinecone index is done initializing
    // todo: timeout and error handling
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
    return host;
}

void InsertBaseTable(Relation heap, Relation index, IndexInfo *indexInfo, char* host, IndexBuildResult *result) {
    PineconeBuildState buildstate;
    int reltuples;
    // initialize the buildstate
    buildstate.indtuples = 0;
    buildstate.json_vectors = cJSON_CreateArray();
    strcpy(buildstate.host, host);
    // iterate through the base table and upsert the vectors to the remote index
    reltuples = table_index_build_scan(heap, index, indexInfo, true, true, pinecone_build_callback, (void *) &buildstate, NULL);
    if (cJSON_GetArraySize(buildstate.json_vectors) > 0) {
        pinecone_bulk_upsert_with_fetch(pinecone_api_key, host, buildstate.json_vectors, pinecone_vectors_per_request, false, NULL);
    }
    cJSON_Delete(buildstate.json_vectors);
    // stats
    result->heap_tuples = reltuples;
    result->index_tuples = buildstate.indtuples;
}

void pinecone_build_callback(Relation index, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state)
{
    PineconeBuildState *buildstate = (PineconeBuildState *) state;
    TupleDesc itup_desc = index->rd_att;
    cJSON *json_vector;
    char* pinecone_id = pinecone_id_from_heap_tid(*tid);
    json_vector = tuple_get_pinecone_vector(itup_desc, values, isnull, pinecone_id);
    cJSON_AddItemToArray(buildstate->json_vectors, json_vector);
    if (cJSON_GetArraySize(buildstate->json_vectors) >= PINECONE_BATCH_SIZE) {
        pinecone_bulk_upsert_with_fetch(pinecone_api_key, buildstate->host, buildstate->json_vectors, pinecone_vectors_per_request, false, NULL);
        cJSON_Delete(buildstate->json_vectors);
        buildstate->json_vectors = cJSON_CreateArray();
    }
    buildstate->indtuples++;
}

void no_buildempty(Relation index){}; // for some reason this is never called even when the base table is empty

/*
 * Create the static meta page
 * Create the buffer meta page
 * Create the buffer head
 */
void InitIndexPages(Relation index, VectorMetric metric, int dimensions, char *pinecone_index_name, char *host, int forkNum) {
    Buffer meta_buf, buffer_meta_buf, buffer_head_buf;
    Page meta_page, buffer_meta_page, buffer_head_page;
    PineconeStaticMetaPage pinecone_static_meta_page;
    PineconeBufferMetaPage pinecone_buffer_meta_page;
    GenericXLogState *state = GenericXLogStart(index);

    // Lock the relation for extension, not really necessary since this is called exactly once in build_index
    LockRelationForExtension(index, ExclusiveLock); 

    // CREATE THE STATIC META PAGE
    meta_buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);
    Assert(BufferGetBlockNumber(meta_buf) == PINECONE_STATIC_METAPAGE_BLKNO);
    meta_page = GenericXLogRegisterBuffer(state, meta_buf, GENERIC_XLOG_FULL_IMAGE);
    PageInit(meta_page, BufferGetPageSize(meta_buf), sizeof(PineconeStaticMetaPageData)); // format as a page
    pinecone_static_meta_page = PineconePageGetStaticMeta(meta_page);
    pinecone_static_meta_page->metric = metric;
    pinecone_static_meta_page->dimensions = dimensions;
    // copy host and pinecone_index_name, checking for length
    if (strlcpy(pinecone_static_meta_page->host, host, sizeof((PineconeStaticMetaPage) 0)) >= sizeof((PineconeStaticMetaPage) 0)) {
        ereport(ERROR, (errcode(ERRCODE_NAME_TOO_LONG), errmsg("Host name too long"), errhint("The host name is %s... and is %d characters long. The maximum length is %d characters.", host, (int) strlen(host), (int) sizeof(pinecone_static_meta_page->host))));
    }
    if (strlcpy(pinecone_static_meta_page->pinecone_index_name, pinecone_index_name, sizeof((PineconeStaticMetaPage) 0)) >= sizeof((PineconeStaticMetaPage) 0)) {
        ereport(ERROR, (errcode(ERRCODE_NAME_TOO_LONG), errmsg("Pinecone index name too long"), errhint("The pinecone index name is %s... and is %d characters long. The maximum length is %d characters.", pinecone_index_name, (int) strlen(pinecone_index_name), (int) sizeof(pinecone_static_meta_page->pinecone_index_name))));
    }

    // CREATE THE BUFFER META PAGE
    buffer_meta_buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);
    Assert(BufferGetBlockNumber(buffer_meta_buf) == PINECONE_BUFFER_METAPAGE_BLKNO);
    buffer_meta_page = GenericXLogRegisterBuffer(state, buffer_meta_buf, GENERIC_XLOG_FULL_IMAGE);
    PageInit(buffer_meta_page, BufferGetPageSize(buffer_meta_buf), sizeof(PineconeBufferMetaPageData)); // format as a page
    pinecone_buffer_meta_page = PineconePageGetBufferMeta(buffer_meta_page);
    // set head, pinecone_tail, and live_tail to START
    pinecone_buffer_meta_page->insert_page = PINECONE_BUFFER_HEAD_BLKNO;
    pinecone_buffer_meta_page->pinecone_page = PINECONE_BUFFER_HEAD_BLKNO;
    pinecone_buffer_meta_page->pinecone_known_live_page = PINECONE_BUFFER_HEAD_BLKNO;
    // set n_tuples to 0
    pinecone_buffer_meta_page->n_tuples = 0;
    pinecone_buffer_meta_page->n_pinecone_tuples = 0;
    pinecone_buffer_meta_page->n_pinecone_live_tuples = 0;
    // set all checkpoint pages to InvalidBlockNumber
    for (int i = 0; i < PINECONE_N_CHECKPOINTS; i++) {
        pinecone_buffer_meta_page->checkpoints[i].page = InvalidBlockNumber;
    }

    // CREATE THE BUFFER HEAD
    buffer_head_buf = ReadBufferExtended(index, forkNum, P_NEW, RBM_NORMAL, NULL);
    Assert(BufferGetBlockNumber(buffer_head_buf) == PINECONE_BUFFER_HEAD_BLKNO);
    buffer_head_page = GenericXLogRegisterBuffer(state, buffer_head_buf, GENERIC_XLOG_FULL_IMAGE);
    PineconePageInit(buffer_head_page, BufferGetPageSize(buffer_head_buf));

    // cleanup
    GenericXLogFinish(state);
    UnlockReleaseBuffer(meta_buf);
    UnlockReleaseBuffer(buffer_meta_buf);
    UnlockReleaseBuffer(buffer_head_buf);
    UnlockRelationForExtension(index, ExclusiveLock);
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

void pinecone_buildempty(Relation index) {}


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

void validate_vector_nonzero(Vector* vector) {
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
    validate_vector_nonzero(vector);
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
    return NULL;
    // char* host = ReadMetaPage(info->index).host;
    // cJSON* ids_to_delete = cJSON_CreateArray();
    // yikes! pinecone makes you read your ids sequentially in pages of 100, so vacuuming a large index is going to be impossible
    // todo: and list isn't even supported on pod-based indexes, so actually it would make sense simply to 
    // use /query to ask for 10K random ids and wait for them to be deleted.
    // but this doesn't quite work either since we need to iterate thru the whole thing.
    // the new plan of just keeping a-T is a lot less hacky. We care about their memory, not their disk space.
    // cJSON* json_vectors = pinecone_list_vectors(pinecone_api_key, ReadMetaPage(info->index).host, 100, NULL);
    // cJSON* json_vector; // todo: do I need to be freeing all my cJSON objects?
    // elog(DEBUG1, "vacuuming json_vectors: %s", cJSON_Print(json_vectors));
    // cJSON_ArrayForEach(json_vector, json_vectors) {
        // char* id = cJSON_GetStringValue(cJSON_GetObjectItem(json_vector, "id"));
        // ItemPointerData heap_tid = pinecone_id_get_heap_tid(id);
        // if (callback(&heap_tid, callback_state)) cJSON_AddItemToArray(ids_to_delete, cJSON_CreateString(id));
    // }
    // elog(DEBUG1, "deleting ids: %s", cJSON_Print(ids_to_delete));
    // pinecone_delete_vectors(pinecone_api_key, host, ids_to_delete);
    // return NULL;
}

IndexBulkDeleteResult *no_vacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats) { return NULL; }

void no_costestimate(PlannerInfo *root, IndexPath *path, double loop_count,
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
                condition_value = cJSON_CreateString(TextDatumGetCString(keys[i].sk_argument));
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
