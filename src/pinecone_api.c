#include <stdio.h>
#include <string.h>
#include <curl/curl.h>
#include "cJSON.h"
#include "pinecone_api.h"

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
    curl_easy_setopt(hnd, CURLOPT_WRITEDATA, stdout);
}

void describe_index(const char *api_key, const char *index_name) {
    CURL *hnd = curl_easy_init();
    CURLcode ret;
    char url[100] = "https://api.pinecone.io/indexes/"; strcat(url, index_name);
    set_curl_options(hnd, api_key, url, "GET");
    ret = curl_easy_perform(hnd);
}

// name, dimension, metric
// serverless: cloud, region
// pod: environment, replicas, pod_type, pods, shards, metadata_config
// the spec should just be passed as a string in json format. We don't need to worry about parsing it.
void create_index(const char *api_key, const char *index_name, const int dimension, const char *metric, const char *spec) {
    CURL *hnd = curl_easy_init();
    CURLcode ret;
    set_curl_options(hnd, api_key, "https://api.pinecone.io/indexes", "POST");
    curl_easy_setopt(hnd, CURLOPT_POSTFIELDS, "{\"metric\":\"cosine\",\"name\":\"m1\",\"dimension\":2,\"spec\":{\"serverless\":{\"cloud\":\"aws\",\"region\":\"us-west-2\"}}}");
    ret = curl_easy_perform(hnd);
}

// querying an index has many options
// namespace, topK, filter, includeValues, includeMetadata, vector, sparseVector, id
// Ultimately I might want to support index-only scans which means I would want to include metadata and values.
// For the prototype, I don't have filtering, metadata so I just support topK and vector.
cJSON* pinecone_api_query_index(const char *api_key, const char *index_host, const int topK, cJSON *query_vector_values) {
    static char response_buffer[40000000]; // 40MB response buffer, this isn't nearly enough when we are returning 10000x1536 values which have to be read in json!!
    FILE *stream = fmemopen(response_buffer, sizeof(response_buffer), "w");
    CURL *hnd = curl_easy_init();
    CURLcode ret;
    cJSON *body = cJSON_CreateObject();
    char url[100] = "https://"; strcat(url, index_host); strcat(url, "/query"); // https://t1-23kshha.svc.apw5-4e34-81fa.pinecone.io/query
    // body has fields topK:topK (integer), vector:query_vector_values
    cJSON_AddItemToObject(body, "topK", cJSON_CreateNumber(topK));
    cJSON_AddItemToObject(body, "vector", query_vector_values);
    // for now set includeValues to true and includeMetadata to false
    cJSON_AddItemToObject(body, "includeValues", cJSON_CreateTrue());
    cJSON_AddItemToObject(body, "includeMetadata", cJSON_CreateFalse());
    set_curl_options(hnd, api_key, url, "POST");
    curl_easy_setopt(hnd, CURLOPT_WRITEDATA, stream);
    curl_easy_setopt(hnd, CURLOPT_POSTFIELDS, cJSON_Print(body));
    ret = curl_easy_perform(hnd);
    // flush stream
    fflush(stream);
    // print response_buffer
    return cJSON_Parse(response_buffer);
}

// to insert vectors call the upsert endpoint
// for now we don't worry about metadata
// a vector has id:string and values:float[]
void pinecone_upsert(const char *api_key, const char *index_host, cJSON *vectors) {
    CURL *hnd = curl_easy_init();
    CURLcode ret;
    cJSON *body = cJSON_CreateObject();
    char *body_str;
    char url[100] = "https://"; strcat(url, index_host); strcat(url, "/vectors/upsert"); // https://t1-23kshha.svc.apw5-4e34-81fa.pinecone.io/vectors/upsert
    cJSON_AddItemToObject(body, "vectors", vectors);
    set_curl_options(hnd, api_key, url, "POST");
    body_str = cJSON_Print(body);
    printf("body: %s\n", body_str);
    curl_easy_setopt(hnd, CURLOPT_POSTFIELDS, body_str);
    ret = curl_easy_perform(hnd);
}

void pinecone_upsert_one(const char *api_key, const char *index_host, cJSON *vector) {
    cJSON *vectors = cJSON_CreateArray();
    cJSON_AddItemToArray(vectors, vector);
    pinecone_upsert(api_key, index_host, vectors);
}