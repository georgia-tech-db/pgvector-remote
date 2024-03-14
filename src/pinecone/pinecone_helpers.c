#include "pinecone_api.h"
#include "pinecone.h"

#include "postgres.h"

#include "fmgr.h"
#include <nodes/execnodes.h>
#include "funcapi.h"
#include "src/cJSON.h"
#include "utils/builtins.h"
#include "executor/spi.h"
#include "fmgr.h"


PGDLLEXPORT PG_FUNCTION_INFO_V1(pinecone_indexes);
Datum
pinecone_indexes(PG_FUNCTION_ARGS) {
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    Tuplestorestate *tupstore;
    TupleDesc tupdesc;
    MemoryContext per_query_ctx, oldcontext;
    cJSON *indexes;
    cJSON *index;

    /* check to see if caller supports us returning a tuplestore */
    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("set-valued function called in context that cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("materialize mode required, but it is not allowed in this context")));

    /* get a tuple descriptor for our result type */
    switch (get_call_result_type(fcinfo, NULL, &tupdesc))
    {
        case TYPEFUNC_COMPOSITE:
            /* success */
            break;
        case TYPEFUNC_RECORD:
            /* failed to determine actual type of RECORD */
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("function returning record called in context that cannot accept type record")));
            break;
        default:
            /* result type isn't a tuple */
            ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                     errmsg("function result type must be a row type")));
            break;
    }

    // create a tuple store and tuple descriptor in the per-query context
    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);
    /* create a tuple store */
    tupdesc = CreateTupleDescCopy(tupdesc);
    tupstore = tuplestore_begin_heap(true, false, 100);
    MemoryContextSwitchTo(oldcontext);

    // validate the api key
    if (pinecone_api_key == NULL || strlen(pinecone_api_key) == 0) {
        ereport(ERROR, (errmsg("Pinecone API key is not set")));
    }
    indexes = list_indexes(pinecone_api_key);
    elog(DEBUG1, "Indexes: %s", cJSON_Print(indexes));

    cJSON_ArrayForEach(index, indexes) {
        Datum values[30];
        bool nulls[30];
        HeapTuple tuple;
        for (int i = 0; i < tupdesc->natts; i++) {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
            char* name = NameStr(attr->attname);
            Oid type = attr->atttypid;
            cJSON *value = cJSON_GetObjectItem(index, name);
            switch (type) {
                case INT4OID:
                    nulls[i] = value == NULL || !cJSON_IsNumber(value);
                    if (!nulls[i]) values[i] = Int32GetDatum((int)cJSON_GetNumberValue(value));
                    break;
                case TEXTOID:
                    nulls[i] = value == NULL || !cJSON_IsString(value);
                    if (!nulls[i]) values[i] = PointerGetDatum(cstring_to_text(cJSON_GetStringValue(value)));
                    break;
                case BOOLOID:
                    nulls[i] = value == NULL || !cJSON_IsBool(value);
                    if (!nulls[i]) values[i] = BoolGetDatum(cJSON_IsTrue(value));
                    break;
                case JSONOID:
                    nulls[i] = value == NULL;
                    if (!nulls[i]) values[i] = PointerGetDatum(cstring_to_text(cJSON_Print(value)));
                    break;
                default:
                    ereport(ERROR, (errmsg("Unsupported type")));
                    break;
            }
        }
        tuple = heap_form_tuple(tupdesc, values, nulls);
        tuplestore_puttuple(tupstore, tuple);
        heap_freetuple(tuple);
    }

    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupstore;
    rsinfo->setDesc = tupdesc;
    // when returning a set, we must return a null Datum
	return (Datum) 0;
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(pinecone_delete_unused_indexes);
Datum
pinecone_delete_unused_indexes(PG_FUNCTION_ARGS) {
    cJSON *indexes;
    cJSON *index;
    int deleted = 0;
    int ret;
    char query[256];

    // validate the api key
    if (pinecone_api_key == NULL || strlen(pinecone_api_key) == 0) {
        ereport(ERROR, (errmsg("Pinecone API key is not set")));
    }

    // validate indexes response
    indexes = list_indexes(pinecone_api_key);
    if (indexes == NULL || !cJSON_IsArray(indexes)) {
        ereport(ERROR, (errmsg("Failed to list indexes. Got response: %s", cJSON_Print(indexes))));
    } else if (cJSON_GetArraySize(indexes) == 0) {
        elog(NOTICE, "No indexes in pinecone");
    }

    // delete each unused index
    // todo: do this concurrently with multicurl
    cJSON_ArrayForEach(index, indexes) {
        cJSON* pinecone_index_name_json;
        Oid index_oid;
        char* pinecone_index_name;
        bool unused = false;

        // get the name of the index in pinecone
        pinecone_index_name_json = cJSON_GetObjectItem(index, "name");
        if (pinecone_index_name_json == NULL || !cJSON_IsString(pinecone_index_name_json)) {
            ereport(ERROR, (errmsg("Index name is not a string")));
        }
        pinecone_index_name = cJSON_GetStringValue(pinecone_index_name_json);

        // deform pinecone_index_name back into index_name, index_oid
        // pinecone_index_name has format ("pgvector-%u-%s-%s", index->rd_id, index_name random_postfix)
        if (sscanf(pinecone_index_name, "pgvector-%u-", &index_oid) != 1) {
            ereport(NOTICE, (errmsg("Failed to parse index name: %s", pinecone_index_name)));
            continue;
        }

        // check if the index's oid exists in pg_class 
        sprintf(query, "SELECT EXISTS( SELECT 1 FROM pg_class WHERE oid = '%u' AND relkind = 'i');", index_oid);
        SPI_connect();
        ret = SPI_execute(query, false, 0);
        if (ret == SPI_OK_SELECT && SPI_processed > 0) {
            TupleDesc tupdesc = SPI_tuptable->tupdesc;
            SPITupleTable *tuptable = SPI_tuptable;
            HeapTuple tuple = tuptable->vals[0];
            bool isnull;
            Datum datum = heap_getattr(tuple, 1, tupdesc, &isnull);
            elog(NOTICE, "Got result: %d", DatumGetBool(datum));
            if (!isnull && DatumGetBool(datum) == false) {
                unused = true;
            }
        } else {
            elog(ERROR, "Failed to execute query");
        }
        SPI_finish();

        // delete the index
        if (unused) pinecone_delete_index(pinecone_api_key, pinecone_index_name);
    }
    PG_RETURN_INT32(deleted);
}

// I need a way to go from an index name to an index oid: I can do this by querying the pg_class table
Oid get_index_oid_from_name(char* index_name) {
    Oid index_oid = 0;
    char query[256];
    int ret;
    sprintf(query, "SELECT oid FROM pg_class WHERE relname = '%s' AND relkind = 'i';", index_name);
    SPI_connect();
    ret = SPI_execute(query, false, 0);
    if (ret == SPI_OK_SELECT && SPI_processed > 0) {
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        SPITupleTable *tuptable = SPI_tuptable;
        HeapTuple tuple = tuptable->vals[0];
        bool isnull;
        Datum datum = heap_getattr(tuple, 1, tupdesc, &isnull);
        if (!isnull) {
            index_oid = DatumGetObjectId(datum);
        }
    } else {
        elog(ERROR, "Failed to execute query");
    }
    SPI_finish();
    return index_oid;
}

PGDLLEXPORT PG_FUNCTION_INFO_V1(pinecone_print_index);
Datum
pinecone_print_index(PG_FUNCTION_ARGS) {
    char* index_name;
    Oid index_oid;
    Relation index;
    index_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
    elog(NOTICE, "Index name: %s", index_name);
    index_oid = get_index_oid_from_name(index_name);
    elog(NOTICE, "Index oid: %u", index_oid);
    index = index_open(index_oid, AccessShareLock);
    #if PG_VERSION_NUM >= 150000
        elog(NOTICE, "Index: %d", index->rd_index->indrelid);
    #endif
    pinecone_print_relation(index);
    index_close(index, AccessShareLock);
    elog(NOTICE, "Index closed. (look no reload)");
    PG_RETURN_VOID();
}

// pinecone_get_index_stats
PGDLLEXPORT PG_FUNCTION_INFO_V1(pinecone_print_index_stats);
Datum
pinecone_print_index_stats(PG_FUNCTION_ARGS) {
    char* index_name;
    PineconeStaticMetaPageData meta;
    cJSON* stats;
    Oid index_oid;
    Relation index;
    index_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
    elog(DEBUG1, "Index name: %s", index_name);
    index_oid = get_index_oid_from_name(index_name);
    elog(DEBUG1, "Index oid: %u", index_oid);
    index = index_open(index_oid, AccessShareLock);
    meta = PineconeSnapshotStaticMeta(index);
    elog(DEBUG1, "host: %s", meta.host);
    stats = pinecone_get_index_stats(pinecone_api_key, meta.host);
    elog(DEBUG1, "Stats: %s", cJSON_Print(stats));
    index_close(index, AccessShareLock);
    elog(DEBUG1, "Index closed");
    PG_RETURN_VOID();
}


// create the mock table
#ifdef PINECONE_MOCK
PGDLLEXPORT PG_FUNCTION_INFO_V1(pinecone_create_mock_table);
Datum
pinecone_create_mock_table(PG_FUNCTION_ARGS) {
    char query[256];
    int ret;
    sprintf(query, "CREATE TABLE pinecone_mock (id SERIAL PRIMARY KEY, url_prefix TEXT, method TEXT, body TEXT, response TEXT, curl_code INT NOT NULL DEFAULT 0);");
    SPI_connect();
    ret = SPI_execute(query, false, 0);
    if (ret != SPI_OK_UTILITY) {
        elog(ERROR, "Failed to create table");
    }
    SPI_finish();
    elog(NOTICE, "Mock table created");
    PG_RETURN_VOID();
}
#endif // PINECONE_MOCK

void lookup_mock_response(CURL* hnd, ResponseData* response_data, CURLcode* curl_code) {
    char query[1024]; // todo: doesn't accomodate large bodies
    int ret;
    char* response = NULL;
    // recover the url from the handle
    char* url;
    curl_easy_getinfo(hnd, CURLINFO_EFFECTIVE_URL, &url);


    SPI_connect();
    // we want startswith the url prefix and we can allow any of url_prefix, method, body if they are null
    sprintf(query, "SELECT response, curl_code FROM pinecone_mock WHERE ('%s' LIKE url_prefix || '%%' OR url_prefix IS NULL) \
                            AND (method IS NULL OR method = '%s') \
                            AND (body IS NULL OR body = '%s');", url, response_data->method, response_data->request_body);
    ret = SPI_execute(query, false, 0);
    if (ret == SPI_OK_SELECT && SPI_processed > 0) {
        TupleDesc tupdesc = SPI_tuptable->tupdesc;
        SPITupleTable *tuptable = SPI_tuptable;
        HeapTuple tuple = tuptable->vals[0];
        bool isnull;
        Datum datum;
        datum = heap_getattr(tuple, 1, tupdesc, &isnull);
        if (!isnull) {
            response = TextDatumGetCString(datum); 
            response_data->data = palloc(strlen(response) + 1);
            strcpy(response_data->data, response);

        }
        datum = heap_getattr(tuple, 2, tupdesc, &isnull);
        if (!isnull) {
            *curl_code = DatumGetInt32(datum);
        }
    } else {
        elog(ERROR, "No matching mock response found for query: %s", query);
    }
    SPI_finish();
}