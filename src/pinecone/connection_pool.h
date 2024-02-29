#ifndef CONNECTION_POOL_H
#define CONNECTION_POOL_H

#include <curl/curl.h>
#include <stdlib.h>
#include <stdbool.h>

#define MAX_CONNECTIONS 10 // Adjust based on your needs

typedef struct {
    CURL* handles[MAX_CONNECTIONS];
    bool inUse[MAX_CONNECTIONS];
    int count;
} ConnectionPool;

ConnectionPool* connectionPoolInit();

CURL* getConnection(ConnectionPool* pool);

void releaseConnection(ConnectionPool* pool, CURL* handle);

void destroyConnectionPool(ConnectionPool* pool);

#endif /* CONNECTION_POOL_H */
