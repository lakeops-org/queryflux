-- Bootstrap Lakekeeper Iceberg tables for the Cerbos demo (Trino inside Compose).
-- S3 endpoint targets RustFS on the Docker network.
--
-- Safe to re-run: never drops a table, and only inserts fixture rows that aren't
-- already present (by id) — running this because ONE demo table is missing must not
-- wipe rows a user added to, or changed in, the OTHER table.

DROP CATALOG IF EXISTS lakekeeper;

CREATE CATALOG lakekeeper USING iceberg
WITH (
    "iceberg.catalog.type" = 'rest',
    "iceberg.rest-catalog.uri" = 'http://lakekeeper:8181/catalog',
    "iceberg.rest-catalog.warehouse" = 'demo',
    "iceberg.rest-catalog.security" = 'NONE',
    "s3.region" = 'local',
    "s3.path-style-access" = 'true',
    "s3.endpoint" = 'http://rustfs:9000',
    "fs.native-s3.enabled" = 'true',
    "s3.aws-access-key" = 'rustfs-root-user',
    "s3.aws-secret-key" = 'rustfs-root-password'
);

CREATE SCHEMA IF NOT EXISTS lakekeeper.demo;

CREATE TABLE IF NOT EXISTS lakekeeper.demo.customers (
  id INTEGER,
  name VARCHAR,
  region VARCHAR,
  ssn VARCHAR
);

INSERT INTO lakekeeper.demo.customers
SELECT * FROM (VALUES
  (1, 'Ana', 'EU', '111-22-3333'),
  (2, 'Ben', 'US', '444-55-6666'),
  (3, 'Cam', 'EU', '777-88-9999')
) AS fixture(id, name, region, ssn)
WHERE NOT EXISTS (
  SELECT 1 FROM lakekeeper.demo.customers c WHERE c.id = fixture.id
);

CREATE TABLE IF NOT EXISTS lakekeeper.demo.payroll (
  id INTEGER,
  name VARCHAR,
  salary INTEGER
);

INSERT INTO lakekeeper.demo.payroll
SELECT * FROM (VALUES
  (1, 'Ana', 120000),
  (2, 'Ben', 95000)
) AS fixture(id, name, salary)
WHERE NOT EXISTS (
  SELECT 1 FROM lakekeeper.demo.payroll p WHERE p.id = fixture.id
);
