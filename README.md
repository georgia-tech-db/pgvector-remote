# pgvector-remote

pgvector-remote is a PostgreSQL extension developed by the Georgia Tech Database Labs. It builds upon the functionality provided by pgvector, introducing seamless integration with dedicated vector stores like Pinecone, with plans to support other vendors in the future.

This extension simplifies the process of storing and retrieving vectors in vector stores while leveraging the power and familiarity of PostgreSQL.

Supports:
- exact and approximate nearest neighbor search
- Metadata filtering with vector similarity search
- L2 distance, inner product, and cosine distance
- vectors are buffered and batch-inserted into remote stores per user-defined sizes
- Seamless data integration and synchronization between pgvector and Pinecone 

## Installation

### Linux and Mac

Compile and install the extension (supports Postgres 12+)

```sh
cd /tmp
git clone --branch feature/remote_indexes https://github.com/georgia-tech-db/pgvector-remote.git
cd pgvector-remote
make
make install # may need sudo
```

See the [installation notes](#installation-notes) if you run into issues

You can also install it with [Docker](#docker)

### Windows

Ensure [C++ support in Visual Studio](https://learn.microsoft.com/en-us/cpp/build/building-on-the-command-line?view=msvc-170#download-and-install-the-tools) is installed, and run:

```cmd
call "C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvars64.bat"
```

Note: The exact path will vary depending on your Visual Studio version and edition

Then use `nmake` to build:

```cmd
set "PGROOT=C:\Program Files\PostgreSQL\16"
git clone --branch feature/remote_indexes https://github.com/georgia-tech-db/pgvector-remote.git
cd pgvector-remote
nmake /F Makefile.win
nmake /F Makefile.win install
```

You can also install it with [Docker](#docker)

## Getting Started

Enable the extension (do this once in each database where you want to use it)

```tsql
CREATE EXTENSION vector;
```

Create a vector column with 3 dimensions

```sql
CREATE TABLE items (id bigserial PRIMARY KEY, embedding vector(3));
```

Insert vectors

```sql
INSERT INTO items (embedding) VALUES ('[1,2,3]'), ('[4,5,6]');
```

Get the nearest neighbors by L2 distance

```sql
SELECT * FROM items ORDER BY embedding <-> '[3,1,2]' LIMIT 5;
```

Also supports inner product (`<#>`) and cosine distance (`<=>`)

Note: `<#>` returns the negative inner product since Postgres only supports `ASC` order index scans on operators

## Storing

Create a new table with a vector column

```sql
CREATE TABLE items (id bigserial PRIMARY KEY, embedding vector(3));
```

Or add a vector column to an existing table

```sql
ALTER TABLE items ADD COLUMN embedding vector(3);
```

Insert vectors

```sql
INSERT INTO items (embedding) VALUES ('[1,2,3]'), ('[4,5,6]');
```

Upsert vectors

```sql
INSERT INTO items (id, embedding) VALUES (1, '[1,2,3]'), (2, '[4,5,6]')
    ON CONFLICT (id) DO UPDATE SET embedding = EXCLUDED.embedding;
```

Update vectors

```sql
UPDATE items SET embedding = '[1,2,3]' WHERE id = 1;
```

Delete vectors

```sql
DELETE FROM items WHERE id = 1;
```

## Indexing

pgvector-remote utilizes Pinecone to create a remote index from vectors stored in PostgreSQL for vector similarity search. To enable metadata filtering alongside vector similarity search, additional metadata must be passed when creating the index.

Add pinecone api key as a system configuration value

```sql
ALTER SYSTEM SET pinecone.api_key = 'xxxxxxxx-xxxx-xxxx-xxxx–xxxxxxxxxxxx';
```

Add an index for each distance function you want to use.

L2 distance

```sql
CREATE INDEX ON items USING pinecone (embedding vector_l2_ops) with (spec = '{"serverless":{"cloud":"aws","region":"us-west-2"}}');
```

Metadata along with vector embedding

```sql
CREATE INDEX ON items USING pinecone (embedding vector_l2_ops, price, quantity) with (spec = '{"serverless":{"cloud":"aws","region":"us-west-2"}}');
```
Here price and quantity are other columns present in postgresql which you want to use as a filter while performing vector similarity search.

Inner product

```sql
CREATE INDEX ON items USING pinecone (embedding vector_ip_ops) with (spec = '{"serverless":{"cloud":"aws","region":"us-west-2"}}');
```

Cosine distance

```sql
CREATE INDEX ON items USING pinecone (embedding vector_cosine_ops) with (spec = '{"serverless":{"cloud":"aws","region":"us-west-2"}}');
```

## Querying

Get the nearest neighbors to a vector

```sql
SELECT * FROM items ORDER BY embedding <-> '[3,1,2]' LIMIT 5;
```

Get the nearest neighbors to a row

```sql
SELECT * FROM items WHERE id != 1 ORDER BY embedding <-> (SELECT embedding FROM items WHERE id = 1) LIMIT 5;
```

Get rows within a certain distance

```sql
SELECT * FROM items WHERE embedding <-> '[3,1,2]' < 5;
```

Note: Combine with `ORDER BY` and `LIMIT` to use an index

#### Distances

Get the distance

```sql
SELECT embedding <-> '[3,1,2]' AS distance FROM items;
```

For inner product, multiply by -1 (since `<#>` returns the negative inner product)

```tsql
SELECT (embedding <#> '[3,1,2]') * -1 AS inner_product FROM items;
```

For cosine similarity, use 1 - cosine distance

```sql
SELECT 1 - (embedding <=> '[3,1,2]') AS cosine_similarity FROM items;
```

#### Aggregates

Average vectors

```sql
SELECT AVG(embedding) FROM items;
```

Average groups of vectors

```sql
SELECT category_id, AVG(embedding) FROM items GROUP BY category_id;
```

### Query Options

pinecone.top_k: Get the top K relevant results from pinecone
pinecone.vectors_per_request: Number of vectors per request
pinecone.requests_per_batch: Number of requests to be sent in one batch
The buffer size is calculated as pinecone.vectors_per_request * pinecone.requests_per_batch
pinecone.max_buffer_scan: Pinecone max buffer search

## Reference

### Vector Type

Each vector takes `4 * dimensions + 8` bytes of storage. Each element is a single precision floating-point number (like the `real` type in Postgres), and all elements must be finite (no `NaN`, `Infinity` or `-Infinity`). Vectors can have up to 16,000 dimensions.

### Vector Operators

Operator | Description | Added
--- | --- | ---
\+ | element-wise addition |
\- | element-wise subtraction |
\* | element-wise multiplication | 0.5.0
<-> | Euclidean distance |
<#> | negative inner product |
<=> | cosine distance |

### Vector Functions

Function | Description | Added
--- | --- | ---
cosine_distance(vector, vector) → double precision | cosine distance |
inner_product(vector, vector) → double precision | inner product |
l2_distance(vector, vector) → double precision | Euclidean distance |
l1_distance(vector, vector) → double precision | taxicab distance | 0.5.0
vector_dims(vector) → integer | number of dimensions |
vector_norm(vector) → double precision | Euclidean norm |

### Aggregate Functions

Function | Description | Added
--- | --- | ---
avg(vector) → vector | average |
sum(vector) → vector | sum | 0.5.0

## Installation Notes

### Postgres Location

If your machine has multiple Postgres installations, specify the path to [pg_config](https://www.postgresql.org/docs/current/app-pgconfig.html) with:

```sh
export PG_CONFIG=/Library/PostgreSQL/16/bin/pg_config
```

Then re-run the installation instructions (run `make clean` before `make` if needed). If `sudo` is needed for `make install`, use:

```sh
sudo --preserve-env=PG_CONFIG make install
```

A few common paths on Mac are:

- EDB installer - `/Library/PostgreSQL/16/bin/pg_config`
- Homebrew (arm64) - `/opt/homebrew/opt/postgresql@16/bin/pg_config`
- Homebrew (x86-64) - `/usr/local/opt/postgresql@16/bin/pg_config`

Note: Replace `16` with your Postgres server version

### Missing Header

If compilation fails with `fatal error: postgres.h: No such file or directory`, make sure Postgres development files are installed on the server.

For Ubuntu and Debian, use:

```sh
sudo apt install postgresql-server-dev-16
```

Note: Replace `16` with your Postgres server version

### Missing SDK

If compilation fails and the output includes `warning: no such sysroot directory` on Mac, reinstall Xcode Command Line Tools.

## Additional Installation Methods

### Docker

Get the [Docker image] with:

```sh
docker pull kslohith17/pgvector-remote:latest
```

This Contains a postgresql along with pgvector-remote configured to run on it.

## Thanks

Thanks to:

- [pgvector: Open-source vector similarity search for Postgres] (https://github.com/pgvector/pgvector)
- [PASE: PostgreSQL Ultra-High-Dimensional Approximate Nearest Neighbor Search Extension](https://dl.acm.org/doi/pdf/10.1145/3318464.3386131)
- [Faiss: A Library for Efficient Similarity Search and Clustering of Dense Vectors](https://github.com/facebookresearch/faiss)
- [Using the Triangle Inequality to Accelerate k-means](https://cdn.aaai.org/ICML/2003/ICML03-022.pdf)
- [k-means++: The Advantage of Careful Seeding](https://theory.stanford.edu/~sergei/papers/kMeansPP-soda.pdf)
- [Concept Decompositions for Large Sparse Text Data using Clustering](https://www.cs.utexas.edu/users/inderjit/public_papers/concept_mlj.pdf)
- [Efficient and Robust Approximate Nearest Neighbor Search using Hierarchical Navigable Small World Graphs](https://arxiv.org/ftp/arxiv/papers/1603/1603.09320.pdf)

## Contributing

Coming soon
