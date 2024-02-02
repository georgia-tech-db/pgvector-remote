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

extern IndexBuildResult *no_build(Relation heap, Relation index, IndexInfo *indexInfo);
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
extern IndexScanDesc default_beginscan(Relation index, int nkeys, int norderbys);
extern void pinecone_rescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys);
extern bool pinecone_gettuple(IndexScanDesc scan, ScanDirection dir);
extern void no_endscan(IndexScanDesc scan);

PGDLLEXPORT Datum pineconehandler(PG_FUNCTION_ARGS);

#endif /* PINECONE_INDEX_AM_H */
