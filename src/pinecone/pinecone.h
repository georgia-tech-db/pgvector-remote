#ifndef PINECONE_INDEX_AM_H
#define PINECONE_INDEX_AM_H

#include "pinecone_api.h"
#include "postgres.h"

// todo: get these out of the header
#include "access/amapi.h"
#include "src/vector.h"
#include "src/cJSON.h"
#include <nodes/execnodes.h>
#include <nodes/pathnodes.h>
#include <utils/array.h>
#include "access/relscan.h"
#include "storage/block.h"

#define PINECONE_DEFAULT_BUFFER_THRESHOLD 2000
#define PINECONE_MIN_BUFFER_THRESHOLD 1
#define PINECONE_MAX_BUFFER_THRESHOLD 10000

#define PINECONE_STATIC_METAPAGE_BLKNO 0
#define PINECONE_BUFFER_METAPAGE_BLKNO 1
#define PINECONE_BUFFER_HEAD_BLKNO 2

#define INVALID_CHECKPOINT_NUMBER -1

#define PineconePageGetOpaque(page)	((PineconeBufferOpaque) PageGetSpecialPointer(page))
#define PineconePageGetStaticMeta(page)	((PineconeStaticMetaPage) PageGetContents(page))
#define PineconePageGetBufferMeta(page)    ((PineconeBufferMetaPage) PageGetContents(page))

// pinecone specific limits

#define PINECONE_NAME_MAX_LENGTH 45
#define PINECONE_HOST_MAX_LENGTH 100

// structs
typedef struct PineconeScanOpaqueData
{
    int dimensions;
    VectorMetric metric;
    bool first;

    // sorting
    Tuplesortstate *sortstate;
    TupleDesc tupdesc;
    TupleTableSlot *slot; // TODO ??
    bool isnull;
    bool more_buffer_tuples;

    // support functions
    FmgrInfo *procinfo;

    // results
    cJSON* pinecone_results;

} PineconeScanOpaqueData;
typedef PineconeScanOpaqueData *PineconeScanOpaque;

extern const char* vector_metric_to_pinecone_metric[VECTOR_METRIC_COUNT];

typedef struct PineconeStaticMetaPageData
{
    int dimensions;
    char host[PINECONE_HOST_MAX_LENGTH + 1];
    char pinecone_index_name[PINECONE_NAME_MAX_LENGTH + 1];
    VectorMetric metric;
} PineconeStaticMetaPageData;
typedef PineconeStaticMetaPageData *PineconeStaticMetaPage;
typedef struct PineconeBuildState
{
    int64 indtuples; // total number of tuples indexed
    cJSON *json_vectors; // array of json vectors
    char host[100];
} PineconeBuildState;

typedef struct PineconeOptions
{
	int32		vl_len_;		/* varlena header (do not touch directly!) */
    int         spec; // spec is a string; this is its offset in the rd_options
}			PineconeOptions;

typedef struct PineconeCheckpoint
{
    int checkpoint_no; // unused by fetch_ids
    BlockNumber blkno; // unused in the page's opaque data
    ItemPointerData tid; // unused in the buffer meta
    int n_preceding_tuples; 
    bool is_checkpoint;
} PineconeCheckpoint;

typedef struct PineconeBufferMetaPageData
{
    // FIFO pointers
    PineconeCheckpoint ready_checkpoint;
    PineconeCheckpoint flush_checkpoint;
    PineconeCheckpoint latest_checkpoint;

    // INSERT PAGE
    BlockNumber insert_page;
    int n_tuples_since_last_checkpoint; // (does not include the tuples in the insert page)
} PineconeBufferMetaPageData;
typedef PineconeBufferMetaPageData *PineconeBufferMetaPage;


typedef struct PineconeBufferOpaqueData
{
    BlockNumber nextblkno;
    BlockNumber prev_checkpoint_blkno;

    // checkpoints for uploading and checking liveness
    PineconeCheckpoint checkpoint;
} PineconeBufferOpaqueData;
typedef PineconeBufferOpaqueData *PineconeBufferOpaque;


// GUC variables
extern char* pinecone_api_key;
extern int pinecone_top_k;
extern int pinecone_vectors_per_request;
extern int pinecone_requests_per_batch;
extern int pinecone_max_buffer_scan;
extern int pinecone_max_fetched_vectors_for_liveness_check;
#define PINECONE_BATCH_SIZE pinecone_vectors_per_request * pinecone_requests_per_batch

// function declarations

// pinecone.c
Datum pineconehandler(PG_FUNCTION_ARGS); // handler
void PineconeInit(void); // GUC and Index Options
bytea * pinecone_options(Datum reloptions, bool validate);
void no_costestimate(PlannerInfo *root, IndexPath *path, double loop_count,
					Cost *indexStartupCost, Cost *indexTotalCost,
					Selectivity *indexSelectivity, double *indexCorrelation,
					double *indexPages);

// build
void generateRandomAlphanumeric(char *s, const int length);
char* get_pinecone_index_name(Relation index);
IndexBuildResult *pinecone_build(Relation heap, Relation index, IndexInfo *indexInfo);
char* CreatePineconeIndexAndWait(Relation index, cJSON* spec_json, VectorMetric metric, char* pinecone_index_name, int dimensions);
void InsertBaseTable(Relation heap, Relation index, IndexInfo *indexInfo, char* host, IndexBuildResult *result);
void pinecone_build_callback(Relation index, ItemPointer tid, Datum *values, bool *isnull, bool tupleIsAlive, void *state);
void InitIndexPages(Relation index, VectorMetric metric, int dimensions, char *pinecone_index_name, char *host, int forkNum);
void pinecone_buildempty(Relation index);
void no_buildempty(Relation index); // for some reason this is never called even when the base table is empty
VectorMetric get_opclass_metric(Relation index);

// insert
bool AppendBufferTupleInCtx(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heapRel, IndexUniqueCheck checkUnique, IndexInfo *indexInfo);
void PineconePageInit(Page page, Size pageSize);
bool AppendBufferTuple(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heapRel);
bool pinecone_insert(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid,
                     Relation heap, IndexUniqueCheck checkUnique, 
#if PG_VERSION_NUM >= 140000
                     bool indexUnchanged, 
#endif
                     IndexInfo *indexInfo);
void FlushToPinecone(Relation index);

// scan
IndexScanDesc pinecone_beginscan(Relation index, int nkeys, int norderbys);
cJSON* pinecone_build_filter(Relation index, ScanKey keys, int nkeys);
void pinecone_rescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys);
void load_buffer_into_sort(Relation index, PineconeScanOpaque so, Datum query_datum, TupleDesc index_tupdesc);
bool pinecone_gettuple(IndexScanDesc scan, ScanDirection dir);
void no_endscan(IndexScanDesc scan);
PineconeCheckpoint* get_checkpoints_to_fetch(Relation index);
PineconeCheckpoint get_best_fetched_checkpoint(Relation index, PineconeCheckpoint* checkpoints, cJSON* fetch_results);
cJSON *fetch_ids_from_checkpoints(PineconeCheckpoint *checkpoints);



// vacuum
IndexBulkDeleteResult *pinecone_bulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
                                     IndexBulkDeleteCallback callback, void *callback_state);
IndexBulkDeleteResult *no_vacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats);

// validate
void pinecone_spec_validator(const char *spec);
void validate_api_key(void);
void validate_vector_nonzero(Vector* vector);
bool no_validate(Oid opclassoid);

// utils
// converting between postgres tuples and json vectors
cJSON* tuple_get_pinecone_vector(TupleDesc tup_desc, Datum *values, bool *isnull, char *vector_id);
cJSON* index_tuple_get_pinecone_vector(Relation index, IndexTuple itup);
char* pinecone_id_from_heap_tid(ItemPointerData heap_tid);
ItemPointerData pinecone_id_get_heap_tid(char *id);
// read and write meta pages
PineconeStaticMetaPageData PineconeSnapshotStaticMeta(Relation index);
PineconeBufferMetaPageData PineconeSnapshotBufferMeta(Relation index);
PineconeBufferOpaqueData PineconeSnapshotBufferOpaque(Relation index, BlockNumber blkno);
void set_buffer_meta_page(Relation index, PineconeCheckpoint* ready_checkpoint, PineconeCheckpoint* flush_checkpoint, PineconeCheckpoint* latest_checkpoint, BlockNumber* insert_page, int* n_tuples_since_last_checkpoint);
char* checkpoint_to_string(PineconeCheckpoint checkpoint);
char* buffer_meta_to_string(PineconeBufferMetaPageData buffer_meta);
char* buffer_opaque_to_string(PineconeBufferOpaqueData buffer_opaque);
void pinecone_print_relation(Relation index);

// helpers
Oid get_index_oid_from_name(char* index_name);


// misc.

#endif // PINECONE_INDEX_AM_H