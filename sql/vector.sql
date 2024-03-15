-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION vector" to load this file. \quit

-- type

CREATE TYPE vector;

CREATE FUNCTION vector_in(cstring, oid, integer) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_out(vector) RETURNS cstring
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_typmod_in(cstring[]) RETURNS integer
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_recv(internal, oid, integer) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_send(vector) RETURNS bytea
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE TYPE vector (
	INPUT     = vector_in,
	OUTPUT    = vector_out,
	TYPMOD_IN = vector_typmod_in,
	RECEIVE   = vector_recv,
	SEND      = vector_send,
	STORAGE   = external
);

-- functions

CREATE FUNCTION l2_distance(vector, vector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION inner_product(vector, vector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION cosine_distance(vector, vector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION l1_distance(vector, vector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_dims(vector) RETURNS integer
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_norm(vector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_add(vector, vector) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_sub(vector, vector) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_mul(vector, vector) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- private functions

CREATE FUNCTION vector_lt(vector, vector) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_le(vector, vector) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_eq(vector, vector) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_ne(vector, vector) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_ge(vector, vector) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_gt(vector, vector) RETURNS bool
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_cmp(vector, vector) RETURNS int4
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_l2_squared_distance(vector, vector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_negative_inner_product(vector, vector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_spherical_distance(vector, vector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_accum(double precision[], vector) RETURNS double precision[]
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_avg(double precision[]) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_combine(double precision[], double precision[]) RETURNS double precision[]
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- pinecone name functions

CREATE FUNCTION vector_l2_pinecone_metric_name() RETURNS int4
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_ip_pinecone_metric_name() RETURNS int4
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_cosine_pinecone_metric_name() RETURNS int4
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- pinecone helper functions

CREATE TYPE pinecone_index_stats AS (
	name text,
	dimension integer,
	metric text,
	host text,
	status json,
	spec json
);

CREATE FUNCTION pinecone_indexes() RETURNS SETOF pinecone_index_stats
	AS 'MODULE_PATHNAME' LANGUAGE C VOLATILE STRICT PARALLEL SAFE;

CREATE FUNCTION pinecone_delete_unused_indexes() RETURNS int4
	AS 'MODULE_PATHNAME' LANGUAGE C VOLATILE STRICT PARALLEL SAFE;

CREATE FUNCTION pinecone_print_index(text) RETURNS int4
	AS 'MODULE_PATHNAME' LANGUAGE C VOLATILE STRICT PARALLEL SAFE;

-- aggregates

CREATE AGGREGATE avg(vector) (
	SFUNC = vector_accum,
	STYPE = double precision[],
	FINALFUNC = vector_avg,
	COMBINEFUNC = vector_combine,
	INITCOND = '{0}',
	PARALLEL = SAFE
);

CREATE AGGREGATE sum(vector) (
	SFUNC = vector_add,
	STYPE = vector,
	COMBINEFUNC = vector_add,
	PARALLEL = SAFE
);

-- cast functions

CREATE FUNCTION vector(vector, integer, boolean) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION array_to_vector(integer[], integer, boolean) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION array_to_vector(real[], integer, boolean) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION array_to_vector(double precision[], integer, boolean) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION array_to_vector(numeric[], integer, boolean) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_to_float4(vector, integer, boolean) RETURNS real[]
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- casts

CREATE CAST (vector AS vector)
	WITH FUNCTION vector(vector, integer, boolean) AS IMPLICIT;

CREATE CAST (vector AS real[])
	WITH FUNCTION vector_to_float4(vector, integer, boolean) AS IMPLICIT;

CREATE CAST (integer[] AS vector)
	WITH FUNCTION array_to_vector(integer[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (real[] AS vector)
	WITH FUNCTION array_to_vector(real[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (double precision[] AS vector)
	WITH FUNCTION array_to_vector(double precision[], integer, boolean) AS ASSIGNMENT;

CREATE CAST (numeric[] AS vector)
	WITH FUNCTION array_to_vector(numeric[], integer, boolean) AS ASSIGNMENT;

-- operators

CREATE OPERATOR <-> (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = l2_distance,
	COMMUTATOR = '<->'
);

CREATE OPERATOR <#> (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_negative_inner_product,
	COMMUTATOR = '<#>'
);

CREATE OPERATOR <=> (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = cosine_distance,
	COMMUTATOR = '<=>'
);

CREATE OPERATOR + (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_add,
	COMMUTATOR = +
);

CREATE OPERATOR - (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_sub,
	COMMUTATOR = -
);

CREATE OPERATOR * (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_mul,
	COMMUTATOR = *
);

CREATE OPERATOR < (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_lt,
	COMMUTATOR = > , NEGATOR = >= ,
	RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

-- should use scalarlesel and scalarlejoinsel, but not supported in Postgres < 11
CREATE OPERATOR <= (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_le,
	COMMUTATOR = >= , NEGATOR = > ,
	RESTRICT = scalarltsel, JOIN = scalarltjoinsel
);

CREATE OPERATOR = (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_eq,
	COMMUTATOR = = , NEGATOR = <> ,
	RESTRICT = eqsel, JOIN = eqjoinsel
);

CREATE OPERATOR <> (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_ne,
	COMMUTATOR = <> , NEGATOR = = ,
	RESTRICT = eqsel, JOIN = eqjoinsel
);

-- should use scalargesel and scalargejoinsel, but not supported in Postgres < 11
CREATE OPERATOR >= (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_ge,
	COMMUTATOR = <= , NEGATOR = < ,
	RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

CREATE OPERATOR > (
	LEFTARG = vector, RIGHTARG = vector, PROCEDURE = vector_gt,
	COMMUTATOR = < , NEGATOR = <= ,
	RESTRICT = scalargtsel, JOIN = scalargtjoinsel
);

-- access methods

CREATE FUNCTION ivfflathandler(internal) RETURNS index_am_handler
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE ACCESS METHOD ivfflat TYPE INDEX HANDLER ivfflathandler;

COMMENT ON ACCESS METHOD ivfflat IS 'ivfflat index access method';

CREATE FUNCTION hnswhandler(internal) RETURNS index_am_handler
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE ACCESS METHOD hnsw TYPE INDEX HANDLER hnswhandler;

COMMENT ON ACCESS METHOD hnsw IS 'hnsw index access method';

CREATE FUNCTION pineconehandler(internal) RETURNS index_am_handler
	AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE ACCESS METHOD pinecone TYPE INDEX HANDLER pineconehandler;

COMMENT ON ACCESS METHOD pinecone IS 'pinecone index access method';

-- opclasses

CREATE OPERATOR CLASS vector_ops
	DEFAULT FOR TYPE vector USING btree AS
	OPERATOR 1 < ,
	OPERATOR 2 <= ,
	OPERATOR 3 = ,
	OPERATOR 4 >= ,
	OPERATOR 5 > ,
	FUNCTION 1 vector_cmp(vector, vector);

CREATE OPERATOR CLASS vector_l2_ops
	DEFAULT FOR TYPE vector USING ivfflat AS
	OPERATOR 1 <-> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 vector_l2_squared_distance(vector, vector),
	FUNCTION 3 l2_distance(vector, vector);

CREATE OPERATOR CLASS vector_ip_ops
	FOR TYPE vector USING ivfflat AS
	OPERATOR 1 <#> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 vector_negative_inner_product(vector, vector),
	FUNCTION 3 vector_spherical_distance(vector, vector),
	FUNCTION 4 vector_norm(vector);

CREATE OPERATOR CLASS vector_cosine_ops
	FOR TYPE vector USING ivfflat AS
	OPERATOR 1 <=> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 vector_negative_inner_product(vector, vector),
	FUNCTION 2 vector_norm(vector),
	FUNCTION 3 vector_spherical_distance(vector, vector),
	FUNCTION 4 vector_norm(vector);

CREATE OPERATOR CLASS vector_l2_ops
	FOR TYPE vector USING hnsw AS
	OPERATOR 1 <-> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 vector_l2_squared_distance(vector, vector);

CREATE OPERATOR CLASS vector_ip_ops
	FOR TYPE vector USING hnsw AS
	OPERATOR 1 <#> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 vector_negative_inner_product(vector, vector);

CREATE OPERATOR CLASS vector_cosine_ops
	FOR TYPE vector USING hnsw AS
	OPERATOR 1 <=> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 vector_negative_inner_product(vector, vector),
	FUNCTION 2 vector_norm(vector);

-- pinecone opclasses

CREATE OPERATOR CLASS vector_l2_ops
	DEFAULT FOR TYPE vector USING pinecone AS
	OPERATOR 1 <-> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 vector_l2_squared_distance(vector, vector),
	FUNCTION 2 vector_l2_pinecone_metric_name();

CREATE OPERATOR CLASS vector_ip_ops
	FOR TYPE vector USING pinecone AS
	OPERATOR 1 <#> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 vector_negative_inner_product(vector, vector),
	FUNCTION 2 vector_ip_pinecone_metric_name();

CREATE OPERATOR CLASS vector_cosine_ops
	FOR TYPE vector USING pinecone AS
	OPERATOR 1 <=> (vector, vector) FOR ORDER BY float_ops,
	FUNCTION 1 cosine_distance(vector, vector),
	FUNCTION 2 vector_cosine_pinecone_metric_name();

-- dummy boolean opclass for pinecone
CREATE OPERATOR CLASS bool_pinecone_ops
	DEFAULT FOR TYPE boolean USING pinecone AS
	OPERATOR 3 = (boolean, boolean),
	OPERATOR 6 != (boolean, boolean);

-- text opclass for pinecone
CREATE OPERATOR CLASS text_pinecone_ops
	DEFAULT FOR TYPE text USING pinecone AS
	OPERATOR 3 = (text, text),
	OPERATOR 6 != (text, text);

-- float opclass for pinecone
CREATE OPERATOR CLASS float_pinecone_ops
	DEFAULT FOR TYPE float8 USING pinecone AS
	OPERATOR 1 < (float8, float8),
	OPERATOR 2 <= (float8, float8),
	OPERATOR 3 = (float8, float8),
	OPERATOR 4 >= (float8, float8),
	OPERATOR 5 > (float8, float8),
	OPERATOR 6 != (float8, float8);

-- we want consistent naming
-- < 1
-- <= 2
-- = 3
-- >= 4
-- > 5
-- != 6

--- svector type

CREATE TYPE svector;

CREATE FUNCTION svector_in(cstring, oid, integer) RETURNS svector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_out(svector) RETURNS cstring
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_typmod_in(cstring[]) RETURNS integer
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_recv(internal, oid, integer) RETURNS svector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_send(svector) RETURNS bytea
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE TYPE svector (
	INPUT     = svector_in,
	OUTPUT    = svector_out,
	TYPMOD_IN = svector_typmod_in,
	RECEIVE   = svector_recv,
	SEND      = svector_send,
	STORAGE   = external
);

-- svector functions

CREATE FUNCTION l2_distance(svector, svector) RETURNS float8
	AS 'MODULE_PATHNAME', 'svector_l2_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION inner_product(svector, svector) RETURNS float8
	AS 'MODULE_PATHNAME', 'svector_inner_product' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION cosine_distance(svector, svector) RETURNS float8
	AS 'MODULE_PATHNAME', 'svector_cosine_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION jaccard_distance(svector, svector) RETURNS float8
	AS 'MODULE_PATHNAME', 'svector_jaccard_distance' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- svector private functions

CREATE FUNCTION svector_l2_squared_distance(svector, svector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_negative_inner_product(svector, svector) RETURNS float8
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- svector cast functions

CREATE FUNCTION svector(svector, integer, boolean) RETURNS svector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION vector_to_svector(vector, integer, boolean) RETURNS svector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION svector_to_vector(svector, integer, boolean) RETURNS vector
	AS 'MODULE_PATHNAME' LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

-- svector casts

CREATE CAST (svector AS svector)
	WITH FUNCTION svector(svector, integer, boolean) AS IMPLICIT;

CREATE CAST (svector AS vector)
	WITH FUNCTION svector_to_vector(svector, integer, boolean) AS IMPLICIT;

CREATE CAST (vector AS svector)
	WITH FUNCTION vector_to_svector(vector, integer, boolean) AS IMPLICIT;

-- svector operators

CREATE OPERATOR <-> (
	LEFTARG = svector, RIGHTARG = svector, PROCEDURE = l2_distance,
	COMMUTATOR = '<->'
);

CREATE OPERATOR <#> (
	LEFTARG = svector, RIGHTARG = svector, PROCEDURE = svector_negative_inner_product,
	COMMUTATOR = '<#>'
);

CREATE OPERATOR <=> (
	LEFTARG = svector, RIGHTARG = svector, PROCEDURE = cosine_distance,
	COMMUTATOR = '<=>'
);