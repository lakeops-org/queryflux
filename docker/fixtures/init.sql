-- Load TPC-H data into Lakekeeper Iceberg tables via Trino.
--
-- Trino (with CATALOG_MANAGEMENT=dynamic) persists the catalog for the
-- container lifetime. StarRocks and DuckDB register their own catalog
-- references at test harness startup.
--
-- Data source: Trino's built-in `tpch` connector. This file uses `tpch.tiny`
-- in every `FROM` clause. The `data-loader` service rewrites `tpch.tiny` to
-- another schema when env `TPCH_SCALE` is set (see docker-compose *.yml).
--
-- Common scales (Trino): `tiny` (~15k orders, default), `sf1` (~1.5M orders),
-- `sf10`, `sf100`, … Larger scales mean much longer load time, more MinIO
-- space, and slower E2E tests (raise timeouts if needed).
-- Example:  TPCH_SCALE=sf1  make dev   or   TPCH_SCALE=sf1  make test-e2e
--
-- All-in-Docker stacks (Trino only inside Compose): use init.docker-network.sql
-- so the Iceberg warehouse S3 endpoint is http://minio:9000.

DROP CATALOG IF EXISTS lakekeeper;

CREATE CATALOG lakekeeper USING iceberg
WITH (
    "iceberg.catalog.type" = 'rest',
    "iceberg.rest-catalog.uri" = 'http://lakekeeper:8181/catalog',
    "iceberg.rest-catalog.warehouse" = 'demo',
    "iceberg.rest-catalog.security" = 'NONE',
    "s3.region" = 'local',
    "s3.path-style-access" = 'true',
    -- Use a hostname that works from both:
    -- - Docker network (where Trino runs)
    -- - the host process running DuckDB (used by QueryFlux tests)
    "s3.endpoint" = 'http://host.docker.internal:19000',
    "fs.native-s3.enabled" = 'true',
    "s3.aws-access-key" = 'minio-root-user',
    "s3.aws-secret-key" = 'minio-root-password'
);

CREATE SCHEMA IF NOT EXISTS lakekeeper.tpch;

-- (Re)create TPCH tables from tpch.<scale> with stable, prefixed column names.
-- We use DROP + CREATE instead of IF NOT EXISTS so repeated test runs
-- don't get stuck with an older schema.
DROP TABLE IF EXISTS lakekeeper.tpch.lineitem;
DROP TABLE IF EXISTS lakekeeper.tpch.partsupp;
DROP TABLE IF EXISTS lakekeeper.tpch.part;
DROP TABLE IF EXISTS lakekeeper.tpch.supplier;
DROP TABLE IF EXISTS lakekeeper.tpch.region;
DROP TABLE IF EXISTS lakekeeper.tpch.customer;
DROP TABLE IF EXISTS lakekeeper.tpch.orders;
DROP TABLE IF EXISTS lakekeeper.tpch.nation;

CREATE TABLE lakekeeper.tpch.region AS
SELECT
    regionkey AS r_regionkey,
    name AS r_name,
    comment AS r_comment
FROM tpch.tiny.region;

CREATE TABLE lakekeeper.tpch.nation AS
SELECT
    nationkey AS n_nationkey,
    name AS n_name,
    regionkey AS n_regionkey,
    comment AS n_comment
FROM tpch.tiny.nation;

CREATE TABLE lakekeeper.tpch.supplier AS
SELECT
    suppkey AS s_suppkey,
    name AS s_name,
    address AS s_address,
    nationkey AS s_nationkey,
    phone AS s_phone,
    acctbal AS s_acctbal,
    comment AS s_comment
FROM tpch.tiny.supplier;

CREATE TABLE lakekeeper.tpch.part AS
SELECT
    partkey AS p_partkey,
    name AS p_name,
    mfgr AS p_mfgr,
    brand AS p_brand,
    type AS p_type,
    size AS p_size,
    container AS p_container,
    retailprice AS p_retailprice,
    comment AS p_comment
FROM tpch.tiny.part;

CREATE TABLE lakekeeper.tpch.partsupp AS
SELECT
    partkey AS ps_partkey,
    suppkey AS ps_suppkey,
    availqty AS ps_availqty,
    supplycost AS ps_supplycost,
    comment AS ps_comment
FROM tpch.tiny.partsupp;

CREATE TABLE lakekeeper.tpch.customer AS
SELECT
    custkey AS c_custkey,
    name AS c_name,
    address AS c_address,
    nationkey AS c_nationkey,
    phone AS c_phone,
    acctbal AS c_acctbal,
    mktsegment AS c_mktsegment,
    comment AS c_comment
FROM tpch.tiny.customer;

CREATE TABLE lakekeeper.tpch.orders AS
SELECT
    orderkey AS o_orderkey,
    custkey AS o_custkey,
    totalprice AS o_totalprice,
    orderdate AS o_orderdate,
    orderpriority AS o_orderpriority,
    clerk AS o_clerk,
    shippriority AS o_shippriority,
    comment AS o_comment,
    orderstatus AS o_orderstatus
FROM tpch.tiny.orders;

CREATE TABLE lakekeeper.tpch.lineitem AS
SELECT
    orderkey AS l_orderkey,
    partkey AS l_partkey,
    suppkey AS l_suppkey,
    linenumber AS l_linenumber,
    quantity AS l_quantity,
    extendedprice AS l_extendedprice,
    discount AS l_discount,
    tax AS l_tax,
    returnflag AS l_returnflag,
    linestatus AS l_linestatus,
    shipdate AS l_shipdate,
    commitdate AS l_commitdate,
    receiptdate AS l_receiptdate,
    shipinstruct AS l_shipinstruct,
    shipmode AS l_shipmode,
    comment AS l_comment
FROM tpch.tiny.lineitem;
