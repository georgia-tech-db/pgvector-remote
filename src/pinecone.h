#ifndef PINECONE_INDEX_AM_H
#define PINECONE_INDEX_AM_H

#include "postgres.h"
#include "access/amapi.h"
#include "vector.h"
#include "pinecone_api.h"
#include "cJSON.h"
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

#define PineconePageGetOpaque(page)	((PineconeBufferOpaque) PageGetSpecialPointer(page))
#define PineconePageGetStaticMeta(page)	((PineconeStaticMetaPage) PageGetContents(page))
#define PineconePageGetBufferMeta(page)    ((PineconeBufferMetaPage) PageGetContents(page))

#define PINECONE_N_CHECKPOINTS 10

#define PINECONE_INSERTION_LOCK_IDENTIFIER 1969841813 // random number, uniquely identifies the pinecone insertion lock



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
    Oid collation; // TODO ??

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

void generateRandomAlphanumeric(char *s, const int length);
extern IndexBuildResult *pinecone_build(Relation heap, Relation index, IndexInfo *indexInfo);
extern void no_buildempty(Relation index);
extern bool pinecone_insert(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heap, IndexUniqueCheck checkUnique
#if PG_VERSION_NUM >= 140000
                            , bool indexUnchanged
#endif
                            , IndexInfo *indexInfo);

ItemPointerData pinecone_id_get_heap_tid(char *id);
char* pinecone_id_from_heap_tid(ItemPointerData heap_tid);
extern IndexBulkDeleteResult *pinecone_bulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats, IndexBulkDeleteCallback callback, void *callback_state);
extern IndexBulkDeleteResult *no_vacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats);
extern void no_costestimate(PlannerInfo *root, IndexPath *path, double loop_count, Cost *indexStartupCost, Cost *indexTotalCost, Selectivity *indexSelectivity, double *indexCorrelation, double *indexPages);
extern bytea * pinecone_options(Datum reloptions, bool validate);
extern bool no_validate(Oid opclassoid);
extern IndexScanDesc pinecone_beginscan(Relation index, int nkeys, int norderbys);
extern void pinecone_rescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys);
extern bool pinecone_gettuple(IndexScanDesc scan, ScanDirection dir);
extern void no_endscan(IndexScanDesc scan);
ItemPointerData id_get_heap_tid(char *id);


// void CreateMetaPage(Relation index, int dimensions, int lists, int forkNum)
extern void pinecone_buildempty(Relation index);
extern void CreateMetaPage(Relation index, int dimensions, char *host, char *pinecone_index_name,  VectorMetric metric, int forkNum);
extern void CreateBufferHead(Relation index, int forkNum);
extern PineconeStaticMetaPageData ReadStaticMetaPage(Relation index);
void		PineconeInit(void);
PGDLLEXPORT Datum pineconehandler(PG_FUNCTION_ARGS);

// buffer
void InsertBufferTupleMemCtx(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heapRel, IndexUniqueCheck checkUnique, IndexInfo *indexInfo);
void InsertBufferTuple(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heapRel);
void incrMetaPageBufferFullness(Relation index);
void setMetaPageBufferFullnessZero(Relation index);
cJSON* get_buffer_pinecone_vectors(Relation index);
void clear_buffer(Relation index);

cJSON* index_tuple_get_pinecone_vector(Relation index, IndexTuple itup);
cJSON* heap_tuple_get_pinecone_vector(Relation heap, HeapTuple htup);
cJSON* tuple_get_pinecone_vector(TupleDesc tup_desc, Datum *values, bool *isnull, char *vector_id);

extern char *pinecone_api_key;
extern void validate_api_key(void);
void pinecone_spec_validator(const char *spec);
void check_vector_nonzero(Vector *vector);

VectorMetric get_opclass_metric(Relation index);
#endif /* PINECONE_INDEX_AM_H */
