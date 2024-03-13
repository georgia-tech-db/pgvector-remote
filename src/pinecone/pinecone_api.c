#include "pinecone_api.h"
#include "pinecone.h"
#include "postgres.h"

#include <stdio.h>
#include <string.h>
#include <curl/curl.h>
#include "src/cJSON.h"

#include <time.h>

#include <stdlib.h>



size_t write_callback(char *contents, size_t size, size_t nmemb, void *userdata) {
    size_t real_size = size * nmemb; // Size of the response
    ResponseData *response_data = (ResponseData *)userdata; // Cast the userdata to the specific structure

    // Attempt to resize the buffer
    char *new_data = realloc(response_data->data, response_data->length + real_size + 1);
    if (new_data != NULL) {
        response_data->data = new_data;
        memcpy(response_data->data + response_data->length, contents, real_size); // Append new data
        response_data->length += real_size;
        response_data->data[response_data->length] = '\0'; // Null terminate the string
    }

    if (response_data->request_body != NULL) {
        // free the request body
        elog(DEBUG1, "Freeing request body");
        free(response_data->request_body);
        response_data->request_body = NULL; // so that subsequent calls to write_callback don't free the request body again
    }

    elog(DEBUG1, "Response (write_callback): %s", contents);

    return real_size;
}

struct curl_slist *create_common_headers(const char *api_key) {
    struct curl_slist *headers = NULL;
    char api_key_header[100] = "Api-Key: "; strcat(api_key_header, api_key);
    headers = curl_slist_append(headers, "accept: application/json");
    headers = curl_slist_append(headers, "content-type: application/json");
    headers = curl_slist_append(headers, api_key_header);
    return headers;
}

void set_curl_options(CURL *hnd, const char *api_key, const char *url, const char *method, ResponseData *response_data) {
    struct curl_slist *headers = create_common_headers(api_key);
    curl_easy_setopt(hnd, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(hnd, CURLOPT_CUSTOMREQUEST, method);
    curl_easy_setopt(hnd, CURLOPT_URL, url);
    curl_easy_setopt(hnd, CURLOPT_WRITEDATA, response_data);
    curl_easy_setopt(hnd, CURLOPT_WRITEFUNCTION, write_callback);
}

// Declare the CURL handle as a global variable
CURL *hnd_t;

cJSON* generic_pinecone_request(const char *api_key, const char *url, const char *method, cJSON *body) {
    // CURL *hnd = curl_easy_init();
    ResponseData response_data = {"", NULL, NULL, 0};
    cJSON *response_json, *error;
    CURLcode ret;

    #ifdef PINECONE_MOCK
    if (strcmp(pinecone_mock_response, "") != 0) {
        elog(NOTICE, "Using mock response");
        response_json = cJSON_Parse(pinecone_mock_response);
        pinecone_mock_response = ""; // reset the mock response
        return response_json;
    }
    #endif

    // Initialize the CURL handle if it hasn't been initialized yet
    if (hnd_t == NULL) {
        elog(NOTICE, "Initializing CURL handle");
        hnd_t = curl_easy_init();
        if (hnd_t == NULL) {
            elog(ERROR, "Failed to initialize CURL handle");
        }
    }

    // make the network request
    set_curl_options(hnd_t, api_key, url, method, &response_data);
    if (body != NULL) {
        curl_easy_setopt(hnd_t, CURLOPT_POSTFIELDS, cJSON_Print(body));
    }
    ret = curl_easy_perform(hnd_t);

    // cleanup
    curl_easy_cleanup(hnd_t);

    // TODO: We need check the ret code in the other endpoints as well
    if (ret != CURLE_OK) {
        elog(ERROR, "curl_easy_perform() failed: %s", curl_easy_strerror(ret));
    }

    response_json = cJSON_Parse(response_data.data);

    if (response_json == NULL) {
        elog(ERROR, "Failed to parse response from Pinecone API. Response: %s", response_data.data);
    }
    // report if json has an error field
    error = cJSON_GetObjectItemCaseSensitive(response_json, "error");
    if (error != NULL) {
        elog(ERROR, "Pinecone API returned an error: %s", cJSON_Print(error));
    }
    return response_json;
}

/*
 * returns a json object with the index's metadata
 * https://docs.pinecone.io/reference/describe_index
 */
cJSON* describe_index(const char *api_key, const char *index_name) {
    char url[100] = "https://api.pinecone.io/indexes/"; strcat(url, index_name);
    return generic_pinecone_request(api_key, url, "GET", NULL);
}

cJSON* pinecone_get_index_stats(const char *api_key, const char *index_host) {
    cJSON* resp;
    clock_t start, stop;
    char url[100] = "https://"; strcat(url, index_host); strcat(url, "/describe_index_stats");
    start = clock();
    resp = generic_pinecone_request(api_key, url, "GET", NULL);
    stop = clock();
    elog(NOTICE, "Getting index stats took %f seconds (praying)", (double)(stop - start) / CLOCKS_PER_SEC);
    return resp;
}

cJSON* list_indexes(const char *api_key) {
    cJSON* response_json;
    response_json = generic_pinecone_request(api_key, "https://api.pinecone.io/indexes", "GET", NULL);
    return cJSON_GetObjectItemCaseSensitive(response_json, "indexes");
}

cJSON* pinecone_delete_vectors(const char *api_key, const char *index_host, cJSON *ids) {
    cJSON *request = cJSON_CreateObject();
    char url[300];
    sprintf(url, "https://%s/vectors/delete", index_host);
    cJSON_AddItemToObject(request, "ids", ids);
    return generic_pinecone_request(api_key, url, "POST", request);
}

cJSON* pinecone_delete_index(const char *api_key, const char *index_name) {
    char url[100] = "https://api.pinecone.io/indexes/"; strcat(url, index_name);
    return generic_pinecone_request(api_key, url, "DELETE", NULL);
}

// delete all vectors in an index
cJSON* pinecone_delete_all(const char *api_key, const char *index_host) {
    char url[300];
    sprintf(url, "https://%s/vectors/delete", index_host);
    return generic_pinecone_request(api_key, url, "POST", cJSON_Parse("{\"deleteAll\": true}"));
}

cJSON* pinecone_list_vectors(const char *api_key, const char *index_host, int limit, char* pagination_token) {
    char url[300];
    if (pagination_token != NULL) {
        sprintf(url, "https://%s/vectors/list?limit=%d&paginationToken=%s", index_host, limit, pagination_token);
    } else {
        sprintf(url, "https://%s/vectors/list?limit=%d", index_host, limit);
    }
    return cJSON_GetObjectItem(generic_pinecone_request(api_key, url, "GET", NULL), "vectors");
}

/* name, dimension, metric
 * serverless: cloud, region
 * pod: environment, replicas, pod_type, pods, shards, metadata_config
 * Refer to https://docs.pinecone.io/reference/create_index
 */
cJSON* pinecone_create_index(const char *api_key, const char *index_name, const int dimension, const char *metric, cJSON *spec) {
    cJSON *request = cJSON_CreateObject();
    cJSON_AddItemToObject(request, "name", cJSON_CreateString(index_name));
    cJSON_AddItemToObject(request, "dimension", cJSON_CreateNumber(dimension));
    cJSON_AddItemToObject(request, "metric", cJSON_CreateString(metric));
    cJSON_AddItemToObject(request, "spec", spec);
    return generic_pinecone_request(api_key, "https://api.pinecone.io/indexes", "POST", request);
}

CURL* multi_hnd_for_query;
cJSON** pinecone_query_with_fetch(const char *api_key, const char *index_host, const int topK, cJSON *query_vector_values, cJSON *filter, bool with_fetch, cJSON* fetch_ids) {
    CURL *query_handle, *fetch_handle;
    cJSON** responses = palloc(2 * sizeof(cJSON*)); // allocate space to return two cJSON* pointers for the query and fetch responses
    ResponseData query_response_data = {"", NULL, NULL, 0};
    ResponseData fetch_response_data = {"", NULL, NULL, 0};
    clock_t start, stop;
    int running;
    if (multi_hnd_for_query == NULL) {
        elog(NOTICE, "Initializing CURL multi handle for QUERY");
        multi_hnd_for_query = curl_multi_init();
        if (multi_hnd_for_query == NULL) {
            elog(ERROR, "Failed to initialize CURL multi handle");
        }
    }

    query_handle = get_pinecone_query_handle(api_key, index_host, topK, query_vector_values, filter, &query_response_data);
    curl_multi_add_handle(multi_hnd_for_query, query_handle);

    if (with_fetch) {
        fetch_handle = get_pinecone_fetch_handle(api_key, index_host, fetch_ids, &fetch_response_data);
        curl_multi_add_handle(multi_hnd_for_query, fetch_handle);
    }

    // todo: does curl let you specify an allocator like cJSON?

    // start time
    start = clock();
    // run the handles
    curl_multi_perform(multi_hnd_for_query, &running);
    while (running) {
        CURLMcode mc;
        int numfds;
        mc = curl_multi_wait(multi_hnd_for_query, NULL, 0, 8000, &numfds);
        if (mc != CURLM_OK) {
            elog(DEBUG1, "curl_multi_wait() failed, code %d.", mc);
            break;
        }
        curl_multi_perform(multi_hnd_for_query, &running);
    }
    // curl_multi_cleanup(multi_hnd_for_query);  // TODO iterate thru and cleanup handles.
    // TODO: figure out exactly what is necessary: deleting multi_cleanup or reusing the same multihandle
    // stop time
    stop = clock();
    elog(NOTICE, "Query and fetch took %f seconds", (double)(stop - start) / CLOCKS_PER_SEC);


    // parse the responses
    start = clock();
    responses[0] = cJSON_Parse(query_response_data.data);
    if (with_fetch) {
        responses[1] = cJSON_Parse(fetch_response_data.data);
    }
    stop = clock();
    elog(NOTICE, "Parsing responses took %f seconds", (double)(stop - start) / CLOCKS_PER_SEC);

    return responses;
}

CURL* multi_handle;
cJSON* pinecone_bulk_upsert(const char *api_key, const char *index_host, cJSON *vectors, int batch_size) {
    cJSON *batches = batch_vectors(vectors, batch_size);
    cJSON *batch;
    CURL* batch_handle;
    ResponseData* response_data = palloc(sizeof(ResponseData) * cJSON_GetArraySize(batches));
    int running;
    if (multi_handle == NULL) {
        elog(NOTICE, "Initializing CURL multi handle for BULK II");
        multi_handle = curl_multi_init();
        if (multi_handle == NULL) {
            elog(ERROR, "Failed to initialize CURL multi handle");
        }
    }

    for (int i = 0; i < cJSON_GetArraySize(batches); i++) {
        batch = cJSON_GetArrayItem(batches, i);
        response_data[i] = (ResponseData) {"", NULL, NULL, 0};
        batch_handle = get_pinecone_upsert_handle(api_key, index_host, cJSON_Duplicate(batch, true), &response_data[i]); // TODO: figure out why i have to deepcopy // because batch goes out of scope
        curl_multi_add_handle(multi_handle, batch_handle);
    }
    cJSON_Delete(batches);

    // run the handles
    curl_multi_perform(multi_handle, &running);
    while (running) {
        CURLMcode mc;
        int numfds;
        mc = curl_multi_wait(multi_handle, NULL, 0, 8000, &numfds);
        if (mc != CURLM_OK) {
            elog(DEBUG1, "curl_multi_wait() failed, code %d.", mc);
            break;
        }
        curl_multi_perform(multi_handle, &running);
    }
    // curl_multi_cleanup(multi_handle);  // TODO iterate thru and cleanup handles.
    // curl_global_cleanup();

    // todo: check the responses from upsert

    // todo: free the response.data
    return NULL;
}

CURL* query_handle;
CURL* get_pinecone_query_handle(const char *api_key, const char *index_host, const int topK, cJSON *query_vector_values, cJSON *filter, ResponseData* response_data) {
    cJSON *body = cJSON_CreateObject();
    char* body_str;
    clock_t start, stop;
    char url[100] = "https://"; strcat(url, index_host); strcat(url, "/query"); // e.g. https://t1-23kshha.svc.apw5-4e34-81fa.pinecone.io/query
    if (query_handle == NULL) {
        elog(NOTICE, "Initializing CURL handle for QUERY");
        query_handle = curl_easy_init();
        if (query_handle == NULL) {
            elog(ERROR, "Failed to initialize CURL handle");
        }
    }
    cJSON_AddItemToObject(body, "topK", cJSON_CreateNumber(topK));
    cJSON_AddItemToObject(body, "vector", query_vector_values);
    cJSON_AddItemToObject(body, "filter", filter);
    cJSON_AddItemToObject(body, "includeValues", cJSON_CreateFalse());
    cJSON_AddItemToObject(body, "includeMetadata", cJSON_CreateFalse());
    elog(DEBUG1, "Querying index %s with payload: %s", index_host, cJSON_Print(body));
    query_handle = curl_easy_init();
    start = clock();
    body_str = cJSON_Print(body);
    cJSON_Delete(body);
    stop = clock();
    // cJSON_Parse(body_str);
    // stop2 = clock();
    elog(NOTICE, "Printing body of query to body_str took %f seconds", (double)(stop - start) / CLOCKS_PER_SEC);
    // elog(NOTICE, "Parsing body_str to cJSON took %f seconds", (double)(stop2 - stop) / CLOCKS_PER_SEC);
    strcpy(response_data->message, "querying index");
    response_data->request_body = body_str;
    set_curl_options(query_handle, api_key, url, "POST", response_data);
    curl_easy_setopt(query_handle, CURLOPT_POSTFIELDS, body_str);
    return query_handle;
}

CURL* get_pinecone_upsert_handle(const char *api_key, const char *index_host, cJSON *vectors, ResponseData* response_data) {
    CURL *hnd = curl_easy_init();
    cJSON *body = cJSON_CreateObject();
    char *body_str;
    // furthermore, we need to be sure to free the memory allocated for response_data.data (by the writeback) after we are done using it
    char url[100] = "https://"; strcat(url, index_host); strcat(url, "/vectors/upsert"); // https://t1-23kshha.svc.apw5-4e34-81fa.pinecone.io/vectors/upsert
    cJSON_AddItemToObject(body, "vectors", vectors);
    set_curl_options(hnd, api_key, url, "POST", response_data);
    body_str = cJSON_Print(body);
    cJSON_Delete(body); // free the cJSON object especially including the vectors
    // save the body_str pointer in response_data so we can free it later in the write_callback
    strcpy(response_data->message, "upserting vectors");
    response_data->request_body = body_str;
    curl_easy_setopt(hnd, CURLOPT_POSTFIELDS, body_str);
    return hnd;
}

CURL* fetch_handle;
CURL* get_pinecone_fetch_handle(const char *api_key, const char *index_host, cJSON* ids, ResponseData* response_data) {
    char url[2048] = "https://"; // we fetch up to 100 vectors and have 12 chars per vector id + &ids= is 17chars/vec
    strcat(url, index_host); strcat(url, "/vectors/fetch?"); // https://t1-23kshha.svc.apw5-4e34-81fa.pinecone.io/vectors/upsert
    if (fetch_handle == NULL) {
        elog(NOTICE, "Initializing CURL handle for FETCH");
        fetch_handle = curl_easy_init();
        if (fetch_handle == NULL) {
            elog(ERROR, "Failed to initialize CURL handle");
        }
    }
    cJSON_ArrayForEach(ids, ids) {
        strcat(url, "ids=");
        strcat(url, cJSON_GetStringValue(ids));
        strcat(url, "&");
    }
    url[strlen(url) - 1] = '\0'; // remove the trailing &
    fetch_handle = curl_easy_init();
    strcpy(response_data->message, "fetching vectors");
    response_data->request_body = NULL;
    set_curl_options(fetch_handle, api_key, url, "GET", response_data);
    return fetch_handle;
}

cJSON* batch_vectors(cJSON *vectors, int batch_size) {
    // given a list of e.g. 1000 vectors, batch them into groups of 100
    cJSON *batches = cJSON_CreateArray();
    cJSON *batch = cJSON_CreateArray();
    int i = 0;
    cJSON *vector;
    cJSON_ArrayForEach(vector, vectors) {
        if (i == batch_size) {
            cJSON_AddItemToArray(batches, batch);
            batch = cJSON_CreateArray();
            i = 0;
        }
        cJSON_AddItemToArray(batch, cJSON_Duplicate(vector, true)); // todo: figure out why i have to deepcopy when using these macros
        i++;
    }
    cJSON_AddItemToArray(batches, batch);
    return batches;
}

