SET enable_seqscan = off;
ALTER SYSTEM SET pinecone.api_key = '5b2c1031-ba58-4acc-a634-9f943d68822c';
CREATE TABLE t (val vector(3));
CREATE INDEX i2 ON t USING pinecone (val vector_l2_ops);
CREATE INDEX i1 ON t USING pinecone (val vector_ip_ops);
CREATE INDEX i3 ON t USING pinecone (val vector_cosine_ops);
DROP TABLE t;
