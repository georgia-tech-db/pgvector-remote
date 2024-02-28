#include "pinecone.h"

#include "utils/guc.h"
#include <access/reloptions.h>


#if PG_VERSION_NUM < 150000
#define MarkGUCPrefixReserved(x) EmitWarningsOnPlaceholders(x)
#endif

char* pinecone_api_key = NULL;
int pinecone_top_k = 1000;
int pinecone_vectors_per_request = 100;
int pinecone_concurrent_requests = 20;
int pinecone_max_buffer_scan = 10000; // maximum number of tuples to search in the buffer

// todo: principled batch sizes. Do we ever want the buffer to be bigger than a multi-insert? Possibly if we want to let the buffer fill up when the remote index is down.
static relopt_kind pinecone_relopt_kind;

void PineconeInit(void)
{
    pinecone_relopt_kind = add_reloption_kind();
    // N.B. The default values are validated when the extension is created, so we have to provide a valid default
    add_string_reloption(pinecone_relopt_kind, "spec",
                            "Specification of the Pinecone Index. Refer to https://docs.pinecone.io/reference/create_index",
                            "{\"serverless\":{\"cloud\":\"aws\",\"region\":\"us-west-2\"}}",
                            &pinecone_spec_validator, 
                            AccessExclusiveLock);
    // todo: allow for specifying a hostname instead of asking to create it
    // todo: you can have a relopts_validator which validates the whole relopt set. This could be used to check that exactly one of spec or host is set
    DefineCustomStringVariable("pinecone.api_key", "Pinecone API key", "Pinecone API key",
                              &pinecone_api_key, "", 
                              PGC_SUSET, // restrict to superusers, takes immediate effect and is not saved in the configuration file 
                              0, NULL, NULL, NULL); // todo: you can have a check_hook that checks that the api key is valid.
    DefineCustomIntVariable("pinecone.top_k", "Pinecone top k", "Pinecone top k",
                            &pinecone_top_k,
                            10000, 1, 10000,
                            PGC_USERSET,
                            0, NULL, NULL, NULL);
    DefineCustomIntVariable("pinecone.vectors_per_request", "Pinecone vectors per request", "Pinecone vectors per request",
                            &pinecone_vectors_per_request,
                            100, 1, 1000,
                            PGC_USERSET,
                            0, NULL, NULL, NULL);
    DefineCustomIntVariable("pinecone.concurrent_requests", "Pinecone concurrent requests", "Pinecone concurrent requests",
                            &pinecone_concurrent_requests,
                            20, 1, 100,
                            PGC_USERSET,
                            0, NULL, NULL, NULL);
    DefineCustomIntVariable("pinecone.max_buffer_scan", "Pinecone max buffer search", "Pinecone max buffer search",
                            &pinecone_max_buffer_scan,
                            10000, 1, 100000,
                            PGC_USERSET,
                            0, NULL, NULL, NULL);
    MarkGUCPrefixReserved("pinecone");
}

void no_costestimate(PlannerInfo *root, IndexPath *path, double loop_count,
					Cost *indexStartupCost, Cost *indexTotalCost,
					Selectivity *indexSelectivity, double *indexCorrelation,
					double *indexPages)
{
    // todo: consider running a health check on the remote index and return infinity if it is not healthy
    if (list_length(path->indexorderbycols) == 0 || linitial_int(path->indexorderbycols) != 0) {
        elog(DEBUG1, "Index must be ordered by the first column");
        *indexTotalCost = 1000000;
        return;
    }
};

bytea * pinecone_options(Datum reloptions, bool validate)
{
    PineconeOptions *opts;
	static const relopt_parse_elt tab[] = {
		{"spec", RELOPT_TYPE_STRING, offsetof(PineconeOptions, spec)},
	};
    opts = (PineconeOptions *) build_reloptions(reloptions, validate,
                                      pinecone_relopt_kind,
                                      sizeof(PineconeOptions),
                                      tab, lengthof(tab));
	return (bytea *) opts;
}

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
    amroutine->amsupport = 2; /* number of support functions */
#if PG_VERSION_NUM >= 130000
    amroutine->amoptsprocnum = 0;
#endif
    amroutine->amcanorder = false;
    amroutine->amcanorderbyop = true;
    amroutine->amcanbackward = false; /* can change direction mid-scan */
    amroutine->amcanunique = false;
    amroutine->amcanmulticol = true; /* TODO: pinecone can support filtered search */
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
    amroutine->ambuild = pinecone_build;
    amroutine->ambuildempty = pinecone_buildempty;
    amroutine->aminsert = pinecone_insert;
    amroutine->ambulkdelete = pinecone_bulkdelete;
    amroutine->amvacuumcleanup = no_vacuumcleanup;
    // used to indicate if we support index-only scans; takes a attno and returns a bool;
    // included cols should always return true since there is little point in an included column if it can't be returned
    amroutine->amcanreturn = NULL; // do we support index-only scans?
    amroutine->amcostestimate = no_costestimate;
    amroutine->amoptions = pinecone_options;
    amroutine->amproperty = NULL;            /* TODO AMPROP_DISTANCE_ORDERABLE */
    amroutine->ambuildphasename = NULL;      // maps build phase number to name
    amroutine->amvalidate = no_validate; // check that the operator class is valid (provide the opclass's object id)
#if PG_VERSION_NUM >= 140000
    amroutine->amadjustmembers = NULL;
#endif
    amroutine->ambeginscan = pinecone_beginscan;
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
