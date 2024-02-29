#include "pinecone.h"


IndexBulkDeleteResult *pinecone_bulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
                                     IndexBulkDeleteCallback callback, void *callback_state)
{
    return NULL;
    // char* host = ReadMetaPage(info->index).host;
    // cJSON* ids_to_delete = cJSON_CreateArray();
    // yikes! pinecone makes you read your ids sequentially in pages of 100, so vacuuming a large index is going to be impossible
    // todo: and list isn't even supported on pod-based indexes, so actually it would make sense simply to 
    // use /query to ask for 10K random ids and wait for them to be deleted.
    // but this doesn't quite work either since we need to iterate thru the whole thing.
    // the new plan of just keeping a-T is a lot less hacky. We care about their memory, not their disk space.
    // cJSON* json_vectors = pinecone_list_vectors(pinecone_api_key, ReadMetaPage(info->index).host, 100, NULL);
    // cJSON* json_vector; // todo: do I need to be freeing all my cJSON objects?
    // elog(DEBUG1, "vacuuming json_vectors: %s", cJSON_Print(json_vectors));
    // cJSON_ArrayForEach(json_vector, json_vectors) {
        // char* id = cJSON_GetStringValue(cJSON_GetObjectItem(json_vector, "id"));
        // ItemPointerData heap_tid = pinecone_id_get_heap_tid(id);
        // if (callback(&heap_tid, callback_state)) cJSON_AddItemToArray(ids_to_delete, cJSON_CreateString(id));
    // }
    // elog(DEBUG1, "deleting ids: %s", cJSON_Print(ids_to_delete));
    // pinecone_delete_vectors(pinecone_api_key, host, ids_to_delete);
    // return NULL;
}

IndexBulkDeleteResult *no_vacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats) { return NULL; }
