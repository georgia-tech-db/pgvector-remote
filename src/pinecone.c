#include "postgres.h"
#include "access/amapi.h"
#include "vector.h"
#include "pinecone_api.h"
#include "pinecone.h"
#include "cJSON.h"
#include <nodes/execnodes.h>
#include <nodes/pathnodes.h>
#include <utils/array.h>
#include "access/relscan.h"

IndexBuildResult *no_build(Relation heap, Relation index, IndexInfo *indexInfo)
{
    IndexBuildResult *result = palloc(sizeof(IndexBuildResult));
    result->heap_tuples = 0;
    result->index_tuples = 0;
    return result;
}

void no_buildempty(Relation index){};

/*
 * Insert a tuple into the index
 */
bool pinecone_insert(Relation index, Datum *values, bool *isnull, ItemPointer heap_tid,
                     Relation heap, IndexUniqueCheck checkUnique
#if PG_VERSION_NUM >= 140000
                     ,
                     bool indexUnchanged
#endif
                     ,
                     IndexInfo *indexInfo)
{
    Vector *vector = DatumGetVector(values[0]);
    cJSON *json_values;
    cJSON *json_vector;
    char vector_id[6 + 1]; // derive the vector_id from the heap_tid
    snprintf(vector_id, sizeof(vector_id), "%02x%02x%02x", heap_tid->ip_blkid.bi_hi, heap_tid->ip_blkid.bi_lo, heap_tid->ip_posid);
    json_values = cJSON_CreateFloatArray(vector->x, vector->dim);
    json_vector = cJSON_CreateObject();
    cJSON_AddItemToObject(json_vector, "id", cJSON_CreateString(vector_id));
    cJSON_AddItemToObject(json_vector, "values", json_values);
    pinecone_upsert_one("5b2c1031-ba58-4acc-a634-9f943d68822c", "t1-23kshha.svc.apw5-4e34-81fa.pinecone.io", json_vector);
    return false;
}

IndexBulkDeleteResult *no_bulkdelete(IndexVacuumInfo *info, IndexBulkDeleteResult *stats,
                                     IndexBulkDeleteCallback callback, void *callback_state)
{
    return NULL;
}

IndexBulkDeleteResult *no_vacuumcleanup(IndexVacuumInfo *info, IndexBulkDeleteResult *stats)
{
    return NULL;
}

void
no_costestimate(PlannerInfo *root, IndexPath *path, double loop_count,
					Cost *indexStartupCost, Cost *indexTotalCost,
					Selectivity *indexSelectivity, double *indexCorrelation,
					double *indexPages)
{};

bytea * no_options(Datum reloptions, bool validate)
{
    return NULL;
}

bool
no_validate(Oid opclassoid)
{
	return true;
}

/*
 * Prepare for an index scan
 */
IndexScanDesc
default_beginscan(Relation index, int nkeys, int norderbys)
{
	IndexScanDesc scan;
	scan = RelationGetIndexScan(index, nkeys, norderbys);
    return scan;
}

/*
 * Start or restart an index scan
 */
void
pinecone_rescan(IndexScanDesc scan, ScanKey keys, int nkeys, ScanKey orderbys, int norderbys)
{
	Vector * vec;
	cJSON *query_vector_values;
	cJSON *pinecone_response;
	cJSON *matches;

	if (orderbys && scan->numberOfOrderBys > 0) {
		vec = DatumGetVector(orderbys[0].sk_argument);
		query_vector_values = cJSON_CreateFloatArray(vec->x, vec->dim);
		pinecone_response = pinecone_api_query_index("5b2c1031-ba58-4acc-a634-9f943d68822c", "t1-23kshha.svc.apw5-4e34-81fa.pinecone.io", 5, query_vector_values);
		// copy pinecone_response to scan opaque
		// response has a matches array, set opaque to the child of matches aka first match
		matches = cJSON_GetObjectItemCaseSensitive(pinecone_response, "matches");
		scan->opaque = matches->child;
	}
}

/*
 * Fetch the next tuple in the given scan
 */
bool
pinecone_gettuple(IndexScanDesc scan, ScanDirection dir)
{
	// interpret scan->opaque as a cJSON object
	cJSON *match = (cJSON *) scan->opaque;
	char *id_str;
	ItemPointerData match_heaptid;
	if (match == NULL) {
		return false;
	}
	// get the id of the match // interpret the id as a string
	id_str = cJSON_GetStringValue(cJSON_GetObjectItemCaseSensitive(match, "id"));
	sscanf(id_str, "%02hx%02hx%02hx", &match_heaptid.ip_blkid.bi_hi, &match_heaptid.ip_blkid.bi_lo, &match_heaptid.ip_posid);
	scan->xs_recheckorderby = false;
	scan->xs_heaptid = match_heaptid;
	// ItemPointer heaptid;
	// scan->xs_heaptid = ItemPointerFromJson(pinecone_response);
	// NEXT
	scan->opaque = match->next;
	return true;
}

void no_endscan(IndexScanDesc scan) {};

/*
 * Define index handler
 *
 * See https://www.postgresql.org/docs/current/index-api.html
 */
PGDLLEXPORT PG_FUNCTION_INFO_V1(pineconehandler);
Datum pineconehandler(PG_FUNCTION_ARGS)
{
    IndexAmRoutine *amroutine = makeNode(IndexAmRoutine);

    amroutine->amstrategies = 0;
    amroutine->amsupport = 0; /* number of support functions */
#if PG_VERSION_NUM >= 130000
    amroutine->amoptsprocnum = 0;
#endif
    amroutine->amcanorder = false;
    amroutine->amcanorderbyop = true;
    amroutine->amcanbackward = false; /* can change direction mid-scan */
    amroutine->amcanunique = false;
    amroutine->amcanmulticol = false; /* TODO: pinecone can support filtered search */
    amroutine->amoptionalkey = true;
    amroutine->amsearcharray = false;
    amroutine->amsearchnulls = false;
    amroutine->amstorage = false;
    amroutine->amclusterable = false;
    amroutine->ampredlocks = false;
    amroutine->amcanparallel = false;
    amroutine->amcaninclude = false;
#if PG_VERSION_NUM >= 130000
    amroutine->amusemaintenanceworkmem = false; /* not used during VACUUM */
    amroutine->amparallelvacuumoptions = 0;
#endif
    amroutine->amkeytype = InvalidOid;

    /* Interface functions */
    amroutine->ambuild = no_build;
    amroutine->ambuildempty = no_buildempty;
    amroutine->aminsert = pinecone_insert;
    amroutine->ambulkdelete = no_bulkdelete;
    amroutine->amvacuumcleanup = no_vacuumcleanup;
    // used to indicate if we support index-only scans; takes a attno and returns a bool;
    // included cols should always return true since there is little point in an included column if it can't be returned
    amroutine->amcanreturn = NULL; // do we support index-only scans?
    amroutine->amcostestimate = no_costestimate;
    amroutine->amoptions = no_options;
    amroutine->amproperty = NULL;            /* TODO AMPROP_DISTANCE_ORDERABLE */
    amroutine->ambuildphasename = NULL;      // maps build phase number to name
    amroutine->amvalidate = no_validate; // check that the operator class is valid (provide the opclass's object id)
#if PG_VERSION_NUM >= 140000
    amroutine->amadjustmembers = NULL;
#endif
    amroutine->ambeginscan = default_beginscan;
    amroutine->amrescan = pinecone_rescan;
    amroutine->amgettuple = pinecone_gettuple;
    amroutine->amgetbitmap = NULL; // an alternative to amgettuple that returns a bitmap of matching tuples
    amroutine->amendscan = no_endscan;
    amroutine->ammarkpos = NULL;
    amroutine->amrestrpos = NULL;

    /* Interface functions to support parallel index scans */
    amroutine->amestimateparallelscan = NULL;
    amroutine->aminitparallelscan = NULL;
    amroutine->amparallelrescan = NULL;

    PG_RETURN_POINTER(amroutine);
}
