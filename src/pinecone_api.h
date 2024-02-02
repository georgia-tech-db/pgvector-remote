#ifndef PINECONE_API_H
#define PINECONE_API_H

#include <curl/curl.h>
#include "cJSON.h"

struct curl_slist *create_common_headers(const char *api_key);
void set_curl_options(CURL *hnd, const char *api_key, const char *url, const char *method);
void describe_index(const char *api_key, const char *index_name);
void create_index(const char *api_key, const char *index_name, const int dimension, const char *metric, const char *spec);
cJSON* pinecone_api_query_index(const char *api_key, const char *index_host, const int topK, cJSON *query_vector_values);
void pinecone_upsert(const char *api_key, const char *index_host, cJSON *vectors);
void pinecone_upsert_one(const char *api_key, const char *index_host, cJSON *vector);

#endif // PINECONE_API_H