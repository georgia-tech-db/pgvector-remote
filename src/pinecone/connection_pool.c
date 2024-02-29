#include "connection_pool.h"

ConnectionPool* connectionPoolInit() {
    ConnectionPool* pool = (ConnectionPool*)malloc(sizeof(ConnectionPool));
    if (!pool) return NULL;

    for (int i = 0; i < MAX_CONNECTIONS; ++i) {
        pool->handles[i] = curl_easy_init();
        pool->inUse[i] = false;
    }
    pool->count = MAX_CONNECTIONS;

    return pool;
}

CURL* getConnection(ConnectionPool* pool) {
    for (int i = 0; i < pool->count; ++i) {
        if (!pool->inUse[i]) {
            pool->inUse[i] = true;
            return pool->handles[i];
        }
    }
    // No available connection, consider waiting or creating a new one. Will need to use pthreads condition variables to make the threads wait
    return NULL;
}

void releaseConnection(ConnectionPool* pool, CURL* handle) {
    for (int i = 0; i < pool->count; ++i) {
        if (pool->handles[i] == handle) {
            pool->inUse[i] = false;
            break;
        }
    }
}

void destroyConnectionPool(ConnectionPool* pool) {
    for (int i = 0; i < pool->count; ++i) {
        if (pool->handles[i]) {
            curl_easy_cleanup(pool->handles[i]);
        }
    }
    free(pool);
}
