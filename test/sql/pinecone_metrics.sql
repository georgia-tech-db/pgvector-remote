SET client_min_messages = 'notice';
CREATE TABLE t (val vector(3));
CREATE INDEX i ON t USING pinecone (val) WITH (metric = "invalid_metric");
CREATE INDEX i ON t USING pinecone (val) WITH (metric = "cosine");
CREATE INDEX i ON t USING pinecone (val vector_cosine_ops) WITH (metric = "euclidean");
