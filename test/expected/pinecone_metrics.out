SET enable_seqscan = off;
CREATE TABLE t (val vector(3));
CREATE INDEX i2 ON t USING pinecone (val vector_l2_ops);
CREATE INDEX i1 ON t USING pinecone (val vector_ip_ops);
CREATE INDEX i3 ON t USING pinecone (val vector_cosine_ops);
DROP TABLE t;
