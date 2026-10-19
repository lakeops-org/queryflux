//! Shared SQL corpus for the per-crate micro-benchmarks (`crates/*/benches`).
//!
//! Included with `#[path = "../../../benches/sql_corpus.rs"] mod sql_corpus;` so every crate
//! benches the same queries and numbers stay comparable across crates. Changing a query here
//! changes the baseline for every benchmark that uses it — CodSpeed then reports it as a new
//! benchmark rather than a regression, so prefer adding a query over editing one.

#![allow(dead_code)]

/// Point lookup — the shape of most dashboard / BI traffic.
pub const POINT: &str = "SELECT id, name, status FROM orders WHERE id = 42";

/// TPC-H Q1 — single-table scan with filters, aggregates and ORDER BY.
pub const TPCH_Q1: &str = "\
SELECT l_returnflag, l_linestatus, \
       sum(l_quantity) AS sum_qty, \
       sum(l_extendedprice) AS sum_base_price, \
       sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price, \
       sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, \
       avg(l_quantity) AS avg_qty, avg(l_extendedprice) AS avg_price, \
       avg(l_discount) AS avg_disc, count(*) AS count_order \
FROM lineitem \
WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY \
GROUP BY l_returnflag, l_linestatus \
ORDER BY l_returnflag, l_linestatus";

/// CTE + multi-way join + window function + subquery — a heavy analytical query.
pub const ANALYTICAL: &str = "\
WITH recent AS ( \
  SELECT o.o_custkey, o.o_orderkey, o.o_totalprice, o.o_orderdate \
  FROM orders o \
  WHERE o.o_orderdate >= DATE '1995-01-01' AND o.o_orderstatus IN ('F', 'O') \
), ranked AS ( \
  SELECT r.*, row_number() OVER (PARTITION BY r.o_custkey ORDER BY r.o_totalprice DESC) AS rn \
  FROM recent r \
) \
SELECT c.c_name, n.n_name, rk.o_orderkey, rk.o_totalprice, \
       sum(l.l_extendedprice * (1 - l.l_discount)) AS revenue \
FROM ranked rk \
JOIN customer c ON c.c_custkey = rk.o_custkey \
JOIN nation n ON n.n_nationkey = c.c_nationkey \
JOIN lineitem l ON l.l_orderkey = rk.o_orderkey \
WHERE rk.rn <= 3 \
  AND c.c_acctbal > (SELECT avg(c_acctbal) FROM customer WHERE c_acctbal > 0) \
  AND l.l_returnflag = 'R' \
GROUP BY c.c_name, n.n_name, rk.o_orderkey, rk.o_totalprice \
ORDER BY revenue DESC \
LIMIT 100";

/// `(name, sql)` pairs, smallest first — the ids used in benchmark names.
pub const ALL: &[(&str, &str)] = &[
    ("point", POINT),
    ("tpch_q1", TPCH_Q1),
    ("analytical", ANALYTICAL),
];
