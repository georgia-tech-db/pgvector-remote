#include "pinecone.h"

#include <access/reloptions.h>


void validate_api_key(void) {
    if (pinecone_api_key == NULL) {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("Pinecone API key not set"),
                 errhint("Set the pinecone API key using the pinecone.api_key GUC. E.g. ALTER SYSTEM SET pinecone.api_key TO 'your-api-key'")));
    }
}

void validate_vector_nonzero(Vector* vector) {
    if (vector_eq_zero_internal(vector)) {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("Invalid vector: zero vector"),
                        errhint("Pinecone insists that dense vectors cannot be zero in all dimensions. I don't know why they do this to you even when your metric isn't cosine.")));
    }
}


void pinecone_spec_validator(const char *spec)
{
    bool empty = strcmp(spec, "") == 0;
    if (empty || cJSON_Parse(spec) == NULL)
    {
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                (empty ? errmsg("Spec cannot be empty") : errmsg("Invalid spec: %s", spec)),
                errhint("Spec should be a valid JSON object e.g. WITH (spec='{\"serverless\":{\"cloud\":\"aws\",\"region\":\"us-west-2\"}}').\n \
                        Refer to https://docs.pinecone.io/reference/create_index")));
    }
}

void pinecone_host_validator(const char *host)
{
    return;
}


bool no_validate(Oid opclassoid) { return true; }

