CREATE EXTERNAL CATALOG IF NOT EXISTS lakekeeper
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "iceberg.catalog.uri" = "http://lakekeeper:8181/catalog",
  "iceberg.catalog.warehouse" = "demo",
  "aws.s3.region" = "local",
  "aws.s3.enable_path_style_access" = "true",
  "aws.s3.endpoint" = "http://rustfs:9000",
  "aws.s3.access_key" = "rustfs-root-user",
  "aws.s3.secret_key" = "rustfs-root-password"
);
