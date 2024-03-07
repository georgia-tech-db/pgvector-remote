SET enable_seqscan = off;
ALTER SYSTEM SET pinecone.api_key = '5b2c1031-ba58-4acc-a634-9f943d68822c';
CREATE TABLE t (id int, val vector(3));
CREATE INDEX i2 ON t USING pinecone (val) WITH (spec = '{"serverless":{"cloud":"aws","region":"us-west-2"}}');
-- Insert two tuples.
INSERT INTO t (id, val) VALUES (1, '[1,0,0]'), VALUES (2, '[1,0,1]');
SELECT id FROM t ORDER BY val <-> '[1, 1, 1]' LIMIT 1;

-- Update one of the tuples and read again.
UPDATE t (val) SET vector = '[1, 1, 1]' WHERE id = 1;
SELECT id FROM t ORDER BY val <-> '[1,1,1]' LIMIT 1;

-- Delete one of the tuples.
DELETE FROM t WHERE id = 1;
SELECT id FROM t ORDER BY val <-> '[1,1,1]' LIMIT 1;

DROP TABLE t;