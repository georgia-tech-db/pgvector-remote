#include <stdio.h>
#include <string.h>
#include <curl/curl.h>
#include "cJSON.h"
#include "pinecone_api.h"
#include "postgres.h"

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

/* https://docs.pinecone.io/reference/describe_index
 * returns a json object with the index's metadata
 */
cJSON* describe_index(const char *api_key, const char *index_name) {
    CURL *hnd = curl_easy_init();
    cJSON *response_json;
    ResponseData response_data = {NULL, 0};
    char url[100] = "https://api.pinecone.io/indexes/"; strcat(url, index_name);
    set_curl_options(hnd, api_key, url, "GET", &response_data);
    curl_easy_perform(hnd);
    response_json = cJSON_Parse(response_data.data);
    return response_json;
}

cJSON* list_indexes(const char *api_key) {
    CURL *hnd = curl_easy_init();
    ResponseData response_data = {NULL, 0};
    cJSON *response_json;
    cJSON* indexes;
    set_curl_options(hnd, api_key, "https://api.pinecone.io/indexes", "GET", &response_data);
    curl_easy_perform(hnd);
    response_json = cJSON_Parse(response_data.data);
    indexes = cJSON_GetObjectItem(response_json, "indexes");
    // todo: error handling
    return indexes;
}

cJSON* generic_pinecone_request(const char *api_key, const char *url, const char *method, cJSON *body) {
    CURL *hnd = curl_easy_init();
    ResponseData response_data = {NULL, 0};
    cJSON *response_json;
    set_curl_options(hnd, api_key, url, method, &response_data);
    if (strcmp(method, "POST") == 0 || strcmp(method, "PUT") == 0) {
        curl_easy_setopt(hnd, CURLOPT_POSTFIELDS, cJSON_Print(body));
    }
    curl_easy_perform(hnd);
    response_json = cJSON_Parse(response_data.data);
    return response_json;
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

cJSON* pinecone_delete_vectors(const char *api_key, const char *index_host, cJSON *ids) {
    cJSON *request = cJSON_CreateObject();
    char url[300];
    sprintf(url, "https://%s/vectors/delete", index_host);
    cJSON_AddItemToObject(request, "ids", ids);
    return generic_pinecone_request(api_key, url, "POST", request);
}

/* name, dimension, metric
 * serverless: cloud, region
 * pod: environment, replicas, pod_type, pods, shards, metadata_config
 * Refer to https://docs.pinecone.io/reference/create_index
 */
cJSON* create_index(const char *api_key, const char *index_name, const int dimension, const char *metric, const char *server_spec) {
    CURL *hnd = curl_easy_init();
    cJSON *body = cJSON_CreateObject();
    cJSON *spec_json = cJSON_Parse(server_spec);
    ResponseData response_data = {NULL, 0};
    cJSON *response_json;
    CURLcode ret;
    // add fields to body
    elog(DEBUG1, "Creating index %s with dimension %d and metric %s", index_name, dimension, metric);
    cJSON_AddItemToObject(body, "name", cJSON_CreateString(index_name));
    cJSON_AddItemToObject(body, "dimension", cJSON_CreateNumber(dimension));
    cJSON_AddItemToObject(body, "metric", cJSON_CreateString(metric));
    cJSON_AddItemToObject(body, "spec", spec_json);
    // set curl options
    set_curl_options(hnd, api_key, "https://api.pinecone.io/indexes", "POST", &response_data);
    curl_easy_setopt(hnd, CURLOPT_POSTFIELDS, cJSON_Print(body));
    ret = curl_easy_perform(hnd);
    if (ret != CURLE_OK) {
        elog(ERROR, "curl_easy_perform() failed: %s\n", curl_easy_strerror(ret));
    }
    curl_easy_cleanup(hnd);
    // return response_data as json
    response_json = cJSON_Parse(response_data.data);
    // print the response
    elog(DEBUG1, "Response (create_index): %s", cJSON_Print(response_json));
    return response_json;
}

cJSON* pinecone_api_query_index(const char *api_key, const char *index_host, const int topK, cJSON *query_vector_values, cJSON *filter) {
    ResponseData response_data = {NULL, 0};
    CURL *hnd = curl_easy_init();
    cJSON *body = cJSON_CreateObject();
    char url[100] = "https://"; strcat(url, index_host); strcat(url, "/query"); // e.g. https://t1-23kshha.svc.apw5-4e34-81fa.pinecone.io/query
    cJSON_AddItemToObject(body, "topK", cJSON_CreateNumber(topK));
    cJSON_AddItemToObject(body, "vector", query_vector_values);
    cJSON_AddItemToObject(body, "filter", filter);
    cJSON_AddItemToObject(body, "includeValues", cJSON_CreateFalse());
    cJSON_AddItemToObject(body, "includeMetadata", cJSON_CreateFalse());
    elog(DEBUG1, "Querying index %s with payload: %s", index_host, cJSON_Print(body));
    set_curl_options(hnd, api_key, url, "POST", &response_data);
    curl_easy_setopt(hnd, CURLOPT_POSTFIELDS, cJSON_Print(body));
    curl_easy_perform(hnd);
    return cJSON_Parse(response_data.data);
}

void pinecone_bulk_upsert(const char *api_key, const char *index_host, cJSON *vectors, int batch_size) {
    cJSON *batches = batch_vectors(vectors, batch_size);
    cJSON *batch;
    CURLM *multi_handle = curl_multi_init();
    cJSON *batch_handle;
    // todo we need to free the actual data in response_data
    ResponseData* response_data = palloc(sizeof(ResponseData) * cJSON_GetArraySize(batches));
    int running;
    int i = 0;
    cJSON_ArrayForEach(batch, batches) {
        response_data[i] = (ResponseData) {NULL, 0};
        batch_handle = get_pinecone_upsert_handle(api_key, index_host, cJSON_Duplicate(batch, true), &response_data[i]); // TODO: figure out why i have to deepcopy // because batch goes out of scope
        curl_multi_add_handle(multi_handle, batch_handle);
        i++;
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


CURL* get_pinecone_upsert_handle(const char *api_key, const char *index_host, cJSON *vectors, ResponseData* response_data) {
    CURL *hnd = curl_easy_init();
    cJSON *body = cJSON_CreateObject();
    char *body_str;
    // this isn't safe because response_data will go out of scope before the writeback function is called
    // furthermore, we need to be sure to free the memory allocated for response_data.data (by the writeback)
    char url[100] = "https://"; strcat(url, index_host); strcat(url, "/vectors/upsert"); // https://t1-23kshha.svc.apw5-4e34-81fa.pinecone.io/vectors/upsert
    cJSON_AddItemToObject(body, "vectors", vectors);
    set_curl_options(hnd, api_key, url, "POST", response_data);
    body_str = cJSON_Print(body);
    curl_easy_setopt(hnd, CURLOPT_POSTFIELDS, body_str);
    return hnd;
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