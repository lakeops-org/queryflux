-- Re-seed Iceberg demo tables (run after init-catalog.sql / compose data-seed).
-- DDL is not evaluated by accessControl (statement.other).

DELETE FROM lakekeeper.demo.customers;

INSERT INTO lakekeeper.demo.customers VALUES
  (1, 'Ana', 'EU', '111-22-3333'),
  (2, 'Ben', 'US', '444-55-6666'),
  (3, 'Cam', 'EU', '777-88-9999');

DELETE FROM lakekeeper.demo.payroll;

INSERT INTO lakekeeper.demo.payroll VALUES
  (1, 'Ana', 120000),
  (2, 'Ben', 95000);
