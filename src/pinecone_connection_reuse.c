#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <curl/curl.h>
#include <time.h>

// Callback function to write response data
size_t write_callback(char *ptr, size_t size, size_t nmemb, void *userdata) {
    return size * nmemb;
}

int main(void) {
    CURL *curl;
    CURL *curl2;
    CURLcode res;

    // Initialize cURL
    curl_global_init(CURL_GLOBAL_ALL);

    // Create a cURL easy handle
    curl = curl_easy_init();
    curl2 = curl_easy_init();

    if(curl) {
        // Set URL for the request
        curl_easy_setopt(curl, CURLOPT_URL, "https://n1-c359nxa.svc.us-east-1-aws.pinecone.io/query");
        curl_easy_setopt(curl2, CURLOPT_URL, "https://n1-c359nxa.svc.us-east-1-aws.pinecone.io/query");

        // Set request headers
        struct curl_slist *headers = NULL;
        headers = curl_slist_append(headers, "Api-Key: 1d41c664-abb2-4372-84af-c95aa09e6405");
        headers = curl_slist_append(headers, "accept: application/json");
        headers = curl_slist_append(headers, "content-type: application/json");

        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl2, CURLOPT_HTTPHEADER, headers);

        // Set options for reusing connections
        curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L); // Enables TCP keep-alive
        curl_easy_setopt(curl, CURLOPT_HTTP_VERSION, CURL_HTTP_VERSION_2); // Use HTTP/2
        curl_easy_setopt(curl, CURLOPT_TCP_KEEPIDLE, 30L);

        // Set request data
        const char *data = "{\"includeValues\": true,\"includeMetadata\": \"false\",\"vector\": [1, 2],\"topK\": 3}";

        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data);
        curl_easy_setopt(curl2, CURLOPT_POSTFIELDS, data);

        // Set write callback function
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
        curl_easy_setopt(curl2, CURLOPT_WRITEFUNCTION, write_callback);

        struct timespec start, end;

        for (int i = 0; i < 5; i++) {
            printf("Request %d\n", i + 1);
            // double start = (double)clock() / CLOCKS_PER_SEC;
            clock_gettime(CLOCK_MONOTONIC, &start);

            // Perform the request
            res = curl_easy_perform(curl);
            if(res != CURLE_OK)
                fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));

            // double end = (double)clock() / CLOCKS_PER_SEC;
            clock_gettime(CLOCK_MONOTONIC, &end);
            // double runtime = end - start;
            double runtime = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
            // printf("Time taken: %.6f seconds\n", runtime);
            printf("Time taken: %.6f seconds\n", runtime);

            // Sleep for 2 seconds
            sleep(20*(i+1));
        }

        for (int i = 0; i < 5; i++) {
            printf("Request %d\n", i + 1);
            // double start = (double)clock() / CLOCKS_PER_SEC;
            clock_gettime(CLOCK_MONOTONIC, &start);

            // Perform the request
            res = curl_easy_perform(curl2);
            if(res != CURLE_OK)
                fprintf(stderr, "curl_easy_perform() failed: %s\n", curl_easy_strerror(res));

            // double end = (double)clock() / CLOCKS_PER_SEC;
            clock_gettime(CLOCK_MONOTONIC, &end);
            // double runtime = end - start;
            double runtime = (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
            // printf("Time taken: %.6f seconds\n", runtime);
            printf("Time taken: %.6f seconds\n", runtime);

            // Sleep for 2 seconds
            sleep(40*(i+1));
        }

        // Cleanup
        curl_easy_cleanup(curl);
        curl_easy_cleanup(curl2);
        curl_slist_free_all(headers);
    }

    // Cleanup global resources
    curl_global_cleanup();

    return 0;
}