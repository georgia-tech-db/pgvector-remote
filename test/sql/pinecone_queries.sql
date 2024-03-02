SET enable_seqscan = off;
ALTER SYSTEM SET pinecone.api_key = '5b2c1031-ba58-4acc-a634-9f943d68822c';
CREATE TABLE t (val vector(3));
CREATE INDEX i2 ON t USING pinecone (val) WITH (spec = '{"serverless":{"cloud":"aws","region":"us-west-2"}}');
-- Insert one tuple and try to read. Buffer wont be flushed at this point.
INSERT INTO t (val) VALUES ('[1,0,0]');
SELECT * FROM t ORDER BY val <-> '[3,3,3]' LIMIT 1;

-- Read tuples after buffer is flushed.
INSERT INTO t (val) VALUES ('[1,2,3]'), ('[1,1,1]');
SELECT pg_sleep(10);
SELECT * FROM t ORDER BY val <-> '[3,3,3]' LIMIT 1;

-- Insert more tuples without flushing buffer, we should merge results from pinecone and buffer at this point.
INSERT INTO t (val) VALUES ('[1,2,4]'), ('[1, 4, 3]');
SELECT * FROM t ORDER BY val <-> '[3,3,3]' DESC LIMIT 1;

-- Test nested queries. 
SELECT COUNT(*) FROM (SELECT * FROM t ORDER BY val <-> '[0,0,0]') t2;

DROP TABLE t;