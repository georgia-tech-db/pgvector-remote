#include <stdio.h>
#include <string.h>
#include <curl/curl.h>
#include "cJSON.h"
#include "pinecone_api.h"
#include "postgres.h"

struct curl_slist *create_common_headers(const char *api_key) {
    struct curl_slist *headers = NULL;
    char api_key_header[100] = "Api-Key: "; strcat(api_key_header, api_key);
    headers = curl_slist_append(headers, "accept: application/json");
    headers = curl_slist_append(headers, "content-type: application/json");
    headers = curl_slist_append(headers, api_key_header);
    return headers;
}

void set_curl_options(CURL *hnd, const char *api_key, const char *url, const char *method) {
    struct curl_slist *headers = create_common_headers(api_key);
    curl_easy_setopt(hnd, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(hnd, CURLOPT_CUSTOMREQUEST, method);
    curl_easy_setopt(hnd, CURLOPT_URL, url);
    // curl_easy_setopt(hnd, CURLOPT_WRITEDATA, stdout);
}

cJSON* describe_index(const char *api_key, const char *index_name) {
    CURL *hnd = curl_easy_init();
    cJSON *response_json;
    char response_data[40960] = "";
    FILE *response_stream = fmemopen(response_data, sizeof(response_data), "w");
    char url[100] = "https://api.pinecone.io/indexes/"; strcat(url, index_name);
    set_curl_options(hnd, api_key, url, "GET");
    curl_easy_setopt(hnd, CURLOPT_WRITEDATA, response_stream);
    curl_easy_perform(hnd);
    fflush(response_stream);
    elog(NOTICE, "Response data: %s", response_data);
    response_json = cJSON_Parse(response_data);
    return response_json;
}

// name, dimension, metric
// serverless: cloud, region
// pod: environment, replicas, pod_type, pods, shards, metadata_config
// the spec should just be passed as a string in json format. We don't need to worry about parsing it.
cJSON* create_index(const char *api_key, const char *index_name, const int dimension, const char *metric, const char *server_spec) {
    CURL *hnd = curl_easy_init();
    CURLcode ret;
    cJSON *body = cJSON_CreateObject();
    cJSON *spec_json;
    char *body_str;
    char response_data[4096] = "";
    FILE *response_stream;
    response_stream = fmemopen(response_data, sizeof(response_data), "w");
    // add fields to body
    elog(NOTICE, "Creating index %s with dimension %d and metric %s", index_name, dimension, metric);
    cJSON_AddItemToObject(body, "name", cJSON_CreateString(index_name));
    cJSON_AddItemToObject(body, "dimension", cJSON_CreateNumber(dimension));
    cJSON_AddItemToObject(body, "metric", cJSON_CreateString(metric));
    // add spec
    spec_json = cJSON_Parse(server_spec);
    cJSON_AddItemToObject(body, "spec", spec_json);
    // convert body to string
    body_str = cJSON_Print(body);
    elog(NOTICE, "Body: %s", body_str);
    // set curl options
    set_curl_options(hnd, api_key, "https://api.pinecone.io/indexes", "POST");
    curl_easy_setopt(hnd, CURLOPT_POSTFIELDS, body_str);
    curl_easy_setopt(hnd, CURLOPT_WRITEDATA, response_stream);
    ret = curl_easy_perform(hnd);
    fflush(response_stream);
    // check if http return code is 422
    elog(NOTICE, "Response code: %d", ret);
    elog(NOTICE, "Response data: %s", response_data);
    // cleanup
    curl_easy_cleanup(hnd);
    fclose(response_stream);
    // return response_data as json
    return cJSON_Parse(response_data);
}

// querying an index has many options
// namespace, topK, filter, includeValues, includeMetadata, vector, sparseVector, id
// Ultimately I might want to support index-only scans which means I would want to include metadata and values.
// For the prototype, I don't have filtering, metadata so I just support topK and vector.
cJSON* pinecone_api_query_index(const char *api_key, const char *index_host, const int topK, cJSON *query_vector_values, cJSON *filter) {
    static char response_buffer[40000000]; // 40MB response buffer, this isn't nearly enough when we are returning 10000x1536 values which have to be read in json!!
    FILE *stream = fmemopen(response_buffer, sizeof(response_buffer), "w");
    CURL *hnd = curl_easy_init();
    CURLcode ret;
    cJSON *body = cJSON_CreateObject();
    char url[100] = "https://"; strcat(url, index_host); strcat(url, "/query"); // https://t1-23kshha.svc.apw5-4e34-81fa.pinecone.io/query
    // body has fields topK:topK (integer), vector:query_vector_values
    cJSON_AddItemToObject(body, "topK", cJSON_CreateNumber(topK));
    cJSON_AddItemToObject(body, "vector", query_vector_values);
    cJSON_AddItemToObject(body, "filter", filter);
    cJSON_AddItemToObject(body, "includeValues", cJSON_CreateFalse());
    cJSON_AddItemToObject(body, "includeMetadata", cJSON_CreateFalse());
    elog(NOTICE, "Querying index %s with payload: %s", index_host, cJSON_Print(body));
    set_curl_options(hnd, api_key, url, "POST");
    curl_easy_setopt(hnd, CURLOPT_WRITEDATA, stream);
    curl_easy_setopt(hnd, CURLOPT_POSTFIELDS, cJSON_Print(body));
    ret = curl_easy_perform(hnd);
    // flush stream
    fflush(stream);
    elog(NOTICE, "Response code: %d", ret);
    elog(NOTICE, "Response data: %s", response_buffer);
    // print response_buffer
    return cJSON_Parse(response_buffer);
}

// to insert vectors call the upsert endpoint
// for now we don't worry about metadata
// a vector has id:string and values:float[]
void pinecone_bulk_upsert(const char *api_key, const char *index_host, cJSON *vectors, int batch_size) {
    cJSON *batches = batch_vectors(vectors, batch_size);
    cJSON *batch;
    CURLM *multi_handle = curl_multi_init();
    cJSON *batch_handle;
    int running;
    cJSON_ArrayForEach(batch, batches) {
        batch_handle = get_pinecone_upsert_handle(api_key, index_host, cJSON_Duplicate(batch, true)); // TODO: figure out why i have to deepcopy
        curl_multi_add_handle(multi_handle, batch_handle);
    }
    // run the handles
    curl_multi_perform(multi_handle, &running);
    while (running) {
        CURLMcode mc;
        int numfds;
        mc = curl_multi_wait(multi_handle, NULL, 0, 8000, &numfds);
        if (mc != CURLM_OK) {
            elog(NOTICE, "curl_multi_wait() failed, code %d.", mc);
            break;
        }
        curl_multi_perform(multi_handle, &running);
    }
    curl_multi_cleanup(multi_handle);  // TODO iterate thru and cleanup handles.
    curl_global_cleanup();
}


CURL* get_pinecone_upsert_handle(const char *api_key, const char *index_host, cJSON *vectors) {
    // char response_data[40960] = "";
    CURL *hnd = curl_easy_init();
    // CURLcode ret;
    cJSON *body = cJSON_CreateObject();
    char *body_str;
    // FILE *response_stream = fmemopen(response_data, sizeof(response_data), "w");
    char url[100] = "https://"; strcat(url, index_host); strcat(url, "/vectors/upsert"); // https://t1-23kshha.svc.apw5-4e34-81fa.pinecone.io/vectors/upsert
    cJSON_AddItemToObject(body, "vectors", vectors);
    set_curl_options(hnd, api_key, url, "POST");
    body_str = cJSON_Print(body);
    // curl_easy_setopt(hnd, CURLOPT_WRITEDATA, response_stream);
    curl_easy_setopt(hnd, CURLOPT_POSTFIELDS, body_str);
    return hnd;
    // ret = curl_easy_perform(hnd);
    // fflush(response_stream);
    // elog(NOTICE, "Response code: %d", ret);
    // elog(NOTICE, "Response data: %s", response_data);
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
        cJSON_AddItemToArray(batch, cJSON_Duplicate(vector, true)); // todo: figure out why i have to deepcopy
        i++;
    }
    cJSON_AddItemToArray(batches, batch);
    return batches;
}