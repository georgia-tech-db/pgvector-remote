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

#define PINECONE_N_CHECKPOINTS 10

#define PINECONE_INSERTION_LOCK_IDENTIFIER 1969841813 // random number, uniquely identifies the pinecone insertion lock


#define PineconePageGetOpaque(page)	((PineconeBufferOpaque) PageGetSpecialPointer(page))
#define PineconePageGetStaticMeta(page)	((PineconeStaticMetaPage) PageGetContents(page))
#define PineconePageGetBufferMeta(page)    ((PineconeBufferMetaPage) PageGetContents(page))

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
    Oid collation; 

    // results
    cJSON* pinecone_results;

} PineconeScanOpaqueData;
typedef PineconeScanOpaqueData *PineconeScanOpaque;

extern const char* vector_metric_to_pinecone_metric[VECTOR_METRIC_COUNT];

typedef struct PineconeCheckpoint {
    BlockNumber page;
    ItemPointerData representative_tid;
} PineconeCheckpoint;

typedef struct PineconeStaticMetaPageData
{
    int dimensions;
    char host[100];
    char pinecone_index_name[60];
    VectorMetric metric;
} PineconeStaticMetaPageData;
typedef PineconeStaticMetaPageData *PineconeStaticMetaPage;

typedef struct PineconeBufferMetaPageData
{
    BlockNumber pinecone_known_live_page;
    BlockNumber pinecone_page;
    BlockNumber insert_page;
    int n_tuples;
    int n_pinecone_tuples;
    int n_pinecone_live_tuples;

    BlockNumber latest_head_checkpoint;
    int n_latest_head_checkpoint;

    PineconeCheckpoint checkpoints[PINECONE_N_CHECKPOINTS];
} PineconeBufferMetaPageData;
typedef PineconeBufferMetaPageData *PineconeBufferMetaPage;

typedef struct PineconeBufferOpaqueData
{
    BlockNumber nextblkno;
    uint16 unused;
    uint16 page_id; // not sure what this is for, but its in the ivf opaques
} PineconeBufferOpaqueData;
typedef PineconeBufferOpaqueData *PineconeBufferOpaque;

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


// GUC variables
extern char* pinecone_api_key;
extern int pinecone_top_k;
extern int pinecone_vectors_per_request;
extern int pinecone_concurrent_requests;
extern int pinecone_max_buffer_scan;
#define PINECONE_BATCH_SIZE pinecone_vectors_per_request * pinecone_concurrent_requests

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
void AdvancePineconeTail(Relation index);
cJSON* get_fetch_ids(PineconeBufferMetaPageData buffer_meta);
void AdvanceLivenessTail(Relation index, cJSON* fetched_ids);

// scan
IndexScanDesc pinecone_beginscan(Relation index, int nkeys, int norderbys);
cJSON* pinecone_build_filter(Relation index, ScanKey keys, int nkeys);
void pinecone_rescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys);
void load_buffer_into_sort(Relation index, PineconeScanOpaque so, Datum query_datum, TupleDesc index_tupdesc);
bool pinecone_gettuple(IndexScanDesc scan, ScanDirection dir);
void no_endscan(IndexScanDesc scan);


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
PineconeStaticMetaPageData GetStaticMetaPageData(Relation index);
PineconeStaticMetaPageData PineconeSnapshotStaticMeta(Relation index);
PineconeBufferMetaPageData PineconeSnapshotBufferMeta(Relation index);
void set_pinecone_page(Relation index, BlockNumber page, int n_new_tuples, ItemPointerData representative_vector_heap_tid);

// misc.

#endif // PINECONE_INDEX_AM_H