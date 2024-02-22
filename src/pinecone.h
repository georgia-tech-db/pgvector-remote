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

#define PINECONE_DEFAULT_BUFFER_THRESHOLD 10
#define PINECONE_MIN_BUFFER_THRESHOLD 1
#define PINECONE_MAX_BUFFER_THRESHOLD 100

// structs
typedef struct PineconeScanOpaqueData
{
    int dimensions;
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


typedef struct PineconeMetaPageData
{
    int dimensions;
    int buffer_fullness;
    int buffer_threshold;
    char host[100];
    char pinecone_index_name[60];
} PineconeMetaPageData;
typedef PineconeMetaPageData *PineconeMetaPage;

typedef struct PineconeBufferOpaqueData
{
    BlockNumber nextblkno;
    uint16 unused;
    uint16 page_id; // not sure what this is for, but its in the ivf opaques
} PineconeBufferOpaqueData;
typedef PineconeBufferOpaqueData *PineconeBufferOpaque;

extern IndexBuildResult *pinecone_build(Relation heap, Relation index, IndexInfo *indexInfo);
extern void no_buildempty(Relation index);
extern bool pinecone_insert(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid, Relation heap, IndexUniqueCheck checkUnique
#if PG_VERSION_NUM >= 140000
                            , bool indexUnchanged
#endif
                            , IndexInfo *indexInfo);
extern IndexBulkDeleteResult *no_bulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats, IndexBulkDeleteCallback callback, void *callback_state);
extern IndexBulkDeleteResult *no_vacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats);
extern void no_costestimate(PlannerInfo *root, IndexPath *path, double loop_count, Cost *indexStartupCost, Cost *indexTotalCost, Selectivity *indexSelectivity, double *indexCorrelation, double *indexPages);
extern bytea * no_options(Datum reloptions, bool validate);
extern bool no_validate(Oid opclassoid);
extern IndexScanDesc pinecone_beginscan(Relation index, int nkeys, int norderbys);
extern void pinecone_rescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys);
extern bool pinecone_gettuple(IndexScanDesc scan, ScanDirection dir);
extern void no_endscan(IndexScanDesc scan);


// void CreateMetaPage(Relation index, int dimensions, int lists, int forkNum)
extern void pinecone_buildempty(Relation index);
extern void CreateMetaPage(Relation index, int dimensions, char *host, char *pinecone_index_name, int buffer_threshold, int forkNum);
extern void CreateBufferHead(Relation index, int forkNum);
extern PineconeMetaPageData ReadMetaPage(Relation index);
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

#endif /* PINECONE_INDEX_AM_H */
