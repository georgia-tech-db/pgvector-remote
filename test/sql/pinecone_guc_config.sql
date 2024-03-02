SET enable_seqscan = off;
ALTER SYSTEM SET pinecone.api_key = '5b2c1031-ba58-4acc-a634-9f943d68822c';
SHOW pinecone.api_key;
SET pinecone.top_k = 10;
SHOW pinecone.top_k;
SET pinecone.vectors_per_request = 100;
SHOW pinecone.vectors_per_request;
SET pinecone.requests_per_batch = 5;
SHOW pinecone.requests_per_batch;
SET pinecone.max_buffer_scan = 1000;
SHOW pinecone.max_buffer_scan;
SET pinecone.max_fetched_vectors_for_liveness_check = 5;
SHOW pinecone.max_fetched_vectors_for_liveness_check;