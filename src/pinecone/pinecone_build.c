#include "pinecone_api.h"
#include "pinecone.h"

#include <access/generic_xlog.h>
#include <storage/bufmgr.h>
#include <access/reloptions.h>

#include <unistd.h>
#include <access/tableam.h>
// LockRelationForExtension in lmgr.h
#include <storage/lmgr.h>


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


const char* vector_metric_to_pinecone_metric[VECTOR_METRIC_COUNT] = {
    "",
    "euclidean",
    "cosine",
    "dotproduct"
};

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


void pinecone_buildempty(Relation index) {}

void no_buildempty(Relation index){}; // for some reason this is never called even when the base table is empty


VectorMetric get_opclass_metric(Relation index)
{
    FmgrInfo *procinfo = index_getprocinfo(index, 1, 2); // lookup the second support function in the opclass for the first attribute
    Oid collation = index->rd_indcollation[0]; // get the collation of the first attribute
    Datum datum = FunctionCall0Coll(procinfo, collation); // call the support function
    return (VectorMetric) DatumGetInt32(datum);
}