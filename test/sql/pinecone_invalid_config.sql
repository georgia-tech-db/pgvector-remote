SET enable_seqscan = off;
ALTER SYSTEM RESET pinecone.api_key;
SELECT pg_reload_conf();
CREATE TABLE t (val vector(3));
CREATE INDEX i2 ON t USING pinecone (val) WITH (spec = '{"serverless":{"cloud":"aws","region":"us-west-2"}}');
ALTER SYSTEM SET pinecone.api_key = '5b2c1031-ba58-4acc-a634-9f943d68822c';
SELECT pg_reload_conf();
CREATE INDEX i2 ON t USING pinecone (val);
DROP TABLE t;