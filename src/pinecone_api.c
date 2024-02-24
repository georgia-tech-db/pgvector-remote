#include <stdio.h>
#include <string.h>
#include <curl/curl.h>
#include "cJSON.h"
#include "pinecone_api.h"
#include "postgres.h"

size_t write_callback(char *contents, size_t size, size_t nmemb, void *userdata) {
    size_t real_size = size * nmemb; // size of the response
    char **str = (char **)userdata; // cast the userdata to a string pointer
    *str = malloc(real_size + 1); // allocate memory for the response
    if (*str == NULL) {
        ereport(ERROR, (errmsg("Failed to allocate memory for response")));
        return 0;
    }
    memcpy(*str, contents, real_size);
    (*str)[real_size] = '\0'; // null terminate the string
    elog(DEBUG1, "Response (write_callback): %s", *str);
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

void set_curl_options(CURL *hnd, const char *api_key, const char *url, const char *method, char** response_data) {
    struct curl_slist *headers = create_common_headers(api_key);
    curl_easy_setopt(hnd, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(hnd, CURLOPT_CUSTOMREQUEST, method);
    curl_easy_setopt(hnd, CURLOPT_URL, url);
    curl_easy_setopt(hnd, CURLOPT_WRITEDATA, response_data); // pass a pointer to the string pointer, so we can reassign it in the callback
    curl_easy_setopt(hnd, CURLOPT_WRITEFUNCTION, write_callback);
    // curl_easy_setopt(hnd, CURLOPT_WRITEDATA, stdout);
}

cJSON* describe_index(const char *api_key, const char *index_name) {
    CURL *hnd = curl_easy_init();
    cJSON *response_json;
    char *response_data = NULL;
    char url[100] = "https://api.pinecone.io/indexes/"; strcat(url, index_name);
    set_curl_options(hnd, api_key, url, "GET", &response_data);
    curl_easy_perform(hnd);
    response_json = cJSON_Parse(response_data);
    return response_json;
}

// name, dimension, metric
// serverless: cloud, region
// pod: environment, replicas, pod_type, pods, shards, metadata_config
// the spec should just be passed as a string in json format. We don't need to worry about parsing it.
cJSON* create_index(const char *api_key, const char *index_name, const int dimension, const char *metric, const char *server_spec) {
    CURL *hnd = curl_easy_init();
    cJSON *body = cJSON_CreateObject();
    cJSON *spec_json;
    char *body_str;
    char *response_data = NULL;
    cJSON *response_json;
    // add fields to body
    elog(DEBUG1, "Creating index %s with dimension %d and metric %s", index_name, dimension, metric);
    cJSON_AddItemToObject(body, "name", cJSON_CreateString(index_name));
    cJSON_AddItemToObject(body, "dimension", cJSON_CreateNumber(dimension));
    cJSON_AddItemToObject(body, "metric", cJSON_CreateString(metric));
    // add spec
    spec_json = cJSON_Parse(server_spec);
    cJSON_AddItemToObject(body, "spec", spec_json);
    // convert body to string
    body_str = cJSON_Print(body);
    elog(DEBUG1, "Body: %s", body_str);
    // set curl options
    set_curl_options(hnd, api_key, "https://api.pinecone.io/indexes", "POST", &response_data);
    curl_easy_setopt(hnd, CURLOPT_POSTFIELDS, body_str);
    curl_easy_perform(hnd);
    // check if http return code is 422
    // cleanup
    curl_easy_cleanup(hnd);
    // return response_data as json
    response_json = cJSON_Parse(response_data);
    return response_json;
}

// querying an index has many options
// namespace, topK, filter, includeValues, includeMetadata, vector, sparseVector, id
// Ultimately I might want to support index-only scans which means I would want to include metadata and values.
// For the prototype, I don't have filtering, metadata so I just support topK and vector.
cJSON* pinecone_api_query_index(const char *api_key, const char *index_host, const int topK, cJSON *query_vector_values, cJSON *filter) {
    char* response_data = NULL;
    CURL *hnd = curl_easy_init();
    cJSON *body = cJSON_CreateObject();
    char url[100] = "https://"; strcat(url, index_host); strcat(url, "/query"); // https://t1-23kshha.svc.apw5-4e34-81fa.pinecone.io/query
    // body has fields topK:topK (integer), vector:query_vector_values
    cJSON_AddItemToObject(body, "topK", cJSON_CreateNumber(topK));
    cJSON_AddItemToObject(body, "vector", query_vector_values);
    cJSON_AddItemToObject(body, "filter", filter);
    cJSON_AddItemToObject(body, "includeValues", cJSON_CreateFalse());
    cJSON_AddItemToObject(body, "includeMetadata", cJSON_CreateFalse());
    elog(DEBUG1, "Querying index %s with payload: %s", index_host, cJSON_Print(body));
    set_curl_options(hnd, api_key, url, "POST", &response_data);
    curl_easy_setopt(hnd, CURLOPT_POSTFIELDS, cJSON_Print(body));
    curl_easy_perform(hnd);
    // print response_buffer
    return cJSON_Parse(response_data);
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
            elog(DEBUG1, "curl_multi_wait() failed, code %d.", mc);
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
    char *response_data = NULL;
    // FILE *response_stream = fmemopen(response_data, sizeof(response_data), "w");
    char url[100] = "https://"; strcat(url, index_host); strcat(url, "/vectors/upsert"); // https://t1-23kshha.svc.apw5-4e34-81fa.pinecone.io/vectors/upsert
    cJSON_AddItemToObject(body, "vectors", vectors);
    set_curl_options(hnd, api_key, url, "POST", &response_data);
    body_str = cJSON_Print(body);
    // curl_easy_setopt(hnd, CURLOPT_WRITEDATA, response_stream);
    curl_easy_setopt(hnd, CURLOPT_POSTFIELDS, body_str);
    // set writeback
    return hnd;
    // ret = curl_easy_perform(hnd);
    // fflush(response_stream);
    // elog(DEBUG1, "Response code: %d", ret);
    // elog(DEBUG1, "Response data: %s", response_data);
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