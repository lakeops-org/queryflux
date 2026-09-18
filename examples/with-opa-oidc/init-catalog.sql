-- Bootstrap Lakekeeper Iceberg tables for the OPA demo (Trino inside Compose).
-- S3 endpoint targets MinIO on the Docker network.

DROP CATALOG IF EXISTS lakekeeper;

CREATE CATALOG lakekeeper USING iceberg
WITH (
    "iceberg.catalog.type" = 'rest',
    "iceberg.rest-catalog.uri" = 'http://lakekeeper:8181/catalog',
    "iceberg.rest-catalog.warehouse" = 'demo',
    "iceberg.rest-catalog.security" = 'NONE',
    "s3.region" = 'local',
    "s3.path-style-access" = 'true',
    "s3.endpoint" = 'http://minio:9000',
    "fs.native-s3.enabled" = 'true',
    "s3.aws-access-key" = 'minio-root-user',
    "s3.aws-secret-key" = 'minio-root-password'
);

CREATE SCHEMA IF NOT EXISTS lakekeeper.demo;

DROP TABLE IF EXISTS lakekeeper.demo.customers;
CREATE TABLE lakekeeper.demo.customers (
  id INTEGER,
  name VARCHAR,
  region VARCHAR,
  ssn VARCHAR
);

INSERT INTO lakekeeper.demo.customers VALUES
  (1, 'Ana', 'EU', '111-22-3333'),
  (2, 'Ben', 'US', '444-55-6666'),
  (3, 'Cam', 'EU', '777-88-9999');

DROP TABLE IF EXISTS lakekeeper.demo.payroll;
CREATE TABLE lakekeeper.demo.payroll (
  id INTEGER,
  name VARCHAR,
  salary INTEGER
);

INSERT INTO lakekeeper.demo.payroll VALUES
  (1, 'Ana', 120000),
  (2, 'Ben', 95000);
