#ifndef PINECONE_API_H
#define PINECONE_API_H

#include <curl/curl.h>
#include "src/cJSON.h"

#define bool _Bool

typedef CURL** CURLHandleList;

typedef struct {
    char *data;
    size_t length;
} ResponseData;

size_t write_callback(char *contents, size_t size, size_t nmemb, void *userdata);
struct curl_slist *create_common_headers(const char *api_key);
void set_curl_options(CURL *hnd, const char *api_key, const char *url, const char *method, ResponseData *response_data);
cJSON* generic_pinecone_request(const char *api_key, const char *url, const char *method, cJSON *body);
cJSON* describe_index(const char *api_key, const char *index_name);
cJSON* list_indexes(const char *api_key);
cJSON* pinecone_delete_vectors(const char *api_key, const char *index_host, cJSON *ids);
cJSON* pinecone_delete_index(const char *api_key, const char *index_name);
cJSON* pinecone_list_vectors(const char *api_key, const char *index_host, int limit, char* pagination_token);
cJSON* pinecone_create_index(const char *api_key, const char *index_name, const int dimension, const char *metric, cJSON *spec);
cJSON** pinecone_query_with_fetch(const char *api_key, const char *index_host, const int topK, cJSON *query_vector_values, cJSON *filter, bool with_fetch, cJSON* fetch_ids);
cJSON* pinecone_bulk_upsert(const char *api_key, const char *index_host, cJSON *vectors, int batch_size);
CURL* get_pinecone_query_handle(const char *api_key, const char *index_host, const int topK, cJSON *query_vector_values, cJSON *filter, ResponseData* response_data);
CURL* get_pinecone_upsert_handle(const char *api_key, const char *index_host, cJSON *vectors, ResponseData* response_data);
CURL* get_pinecone_fetch_handle(const char *api_key, const char *index_host, cJSON* ids, ResponseData* response_data);
cJSON* batch_vectors(cJSON *vectors, int batch_size);

#endif // PINECONE_API_H