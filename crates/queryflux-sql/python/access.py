import json
import re
import sqlglot
from sqlglot import expressions as exp
from sqlglot.optimizer.qualify import qualify


def _cte_names(tree):
    names = set()
    for cte in tree.find_all(exp.CTE):
        a = cte.alias_or_name
        if a:
            names.add(a.lower())
    return names


def _table_key(t):
    cat = t.catalog or ""
    db = t.db or ""
    name = t.name or ""
    return (cat, db, name)


def _qualified(t):
    db = t.db
    return (db + "." + t.name) if db else t.name


_KINDS = {
    "TABLE": "table",
    "VIEW": "view",
    "SCHEMA": "schema",
    "DATABASE": "catalog",
    "FUNCTION": "function",
    "PROCEDURE": "procedure",
}


def _unwrap(node):
    if isinstance(node, exp.Schema):
        node = node.this
    if isinstance(node, exp.UserDefinedFunction):
        node = node.this
    return node if isinstance(node, exp.Table) else None


def _write_targets(tree):
    """([(Table node, kind)], embeds_reads, replaces).

    The object(s) a statement writes to, with their kind (`table`/`view`/`schema`/`catalog`):
    INSERT/UPDATE/DELETE/MERGE/TRUNCATE and CREATE/DROP/ALTER of a table, view, schema or
    database (a database is reported as a catalog). Every *other* table a statement mentions
    is read. `embeds_reads` is true for queries and for statements that contain one
    (`INSERT … SELECT`, `CREATE … AS <query>`); `replaces` is true for `CREATE OR REPLACE`.
    Statements that only name an object without reading it (`DESCRIBE`, ...) embed no reads.
    """
    if isinstance(tree, exp.TruncateTable):
        return [(t, "table") for t in tree.expressions if isinstance(t, exp.Table)], False, False
    if isinstance(tree, exp.Select) and tree.args.get("into") is not None:
        node = _unwrap(tree.args["into"].this)
        return ([(node, "table")] if node is not None else []), True, False
    if isinstance(tree, exp.Copy):
        # `COPY t FROM …` loads t; `COPY t|(query) TO …` and Snowflake's `COPY INTO @stage FROM
        # t` only read. A Snowflake stage (`@stage`) is a location, not a catalog table.
        node = _unwrap(tree.this) if tree.args.get("kind") else None
        if node is not None and not node.name.startswith("@"):
            return [(node, "table")], False, False
        return [], True, False
    if isinstance(tree, (exp.Insert, exp.Update, exp.Delete, exp.Merge)):
        node = _unwrap(tree.this)
        return ([(node, "table")] if node is not None else []), True, False
    kind = _KINDS.get(str(tree.args.get("kind") or "").upper())
    if isinstance(tree, exp.Create):
        node = _unwrap(tree.this)
        targets = [(node, kind)] if kind and node is not None else []
        return (
            targets,
            isinstance(tree.args.get("expression"), exp.Query),
            bool(tree.args.get("replace")),
        )
    if isinstance(tree, exp.Drop):
        # Older sqlglot releases (e.g. 30.17.x, still >=30.0.3) put a single-object DROP's
        # target in `tree.this` rather than the `tables` list — `DROP TABLE t`, `DROP
        # PROCEDURE p`, etc. all parsed that way. Newer releases moved it to `tables`
        # (also how a genuine multi-table `DROP TABLE a, b` is represented in either
        # release). Falling back to `.this` when `tables` is empty covers both shapes
        # instead of pinning extraction to one specific sqlglot release's internals.
        nodes = [n for n in tree.args.get("tables", []) if isinstance(n, exp.Table)]
        if not nodes:
            node = _unwrap(tree.this)
            if node is not None:
                nodes = [node]
        return ([(n, kind) for n in nodes] if kind else []), False, False
    if isinstance(tree, exp.Alter):
        node = _unwrap(tree.this)
        ok = kind in ("table", "view") and node is not None
        return ([(node, kind)] if ok else []), False, False
    return [], isinstance(tree, exp.Query), False


def _set_assignment_columns(expressions):
    """Column names assigned by a list of `col = expr` nodes (an UPDATE's SET list, or an
    ON CONFLICT/ON DUPLICATE KEY DO UPDATE SET list)."""
    return {
        e.this.name
        for e in expressions or []
        if isinstance(e, exp.EQ) and isinstance(e.this, exp.Column)
    }


def _written_columns(tree):
    """Columns an INSERT column list / UPDATE ... SET / CREATE TABLE (...) names, sorted;
    None = the whole row (DELETE, MERGE, TRUNCATE, an INSERT without a column list, or any
    statement that names no columns)."""
    cols = None
    if isinstance(tree, (exp.Insert, exp.Copy)):
        if isinstance(tree.this, exp.Schema):
            cols = {c.name for c in tree.this.expressions if getattr(c, "name", None)}
        if isinstance(tree, exp.Insert):
            # `INSERT ... ON CONFLICT DO UPDATE SET ...` (or MySQL's `ON DUPLICATE KEY UPDATE`,
            # parsed into the same `conflict` arg) can write columns the insert column list
            # never named — those are also write-target columns a policy must see. A
            # whole-row insert (`cols` still None here) already covers them; only a named
            # column list needs extending. `COPY` has no such clause.
            conflict = tree.args.get("conflict")
            if conflict is not None and cols is not None:
                cols |= _set_assignment_columns(conflict.expressions)
    elif isinstance(tree, exp.Update):
        cols = _set_assignment_columns(tree.expressions)
    elif isinstance(tree, exp.Create) and isinstance(tree.this, exp.Schema):
        cols = {c.name for c in tree.this.expressions if isinstance(c, exp.ColumnDef)}
    return sorted(cols) if cols else None


def _entry(kind, parts, columns=None, value=None):
    """A write-target / administrative resource. `parts` is the dotted name, outermost first:
    a schema is `[catalog?, schema]`, a catalog `[catalog]`, everything else
    `[catalog?, schema?, name]` (a role or session setting is just `[name]`)."""
    parts = [p for p in parts if p]
    entry = {"columns": columns, "target": True, "kind": kind, "value": value}
    if kind == "schema":
        entry.update(
            catalog=parts[-2] if len(parts) > 1 else None,
            schema=parts[-1] if parts else None,
            table="",
            columns=None,
        )
    elif kind == "catalog":
        entry.update(catalog=parts[-1] if parts else None, schema=None, table="", columns=None)
    else:
        entry.update(
            catalog=parts[-3] if len(parts) > 2 else None,
            schema=parts[-2] if len(parts) > 1 else None,
            table=parts[-1] if parts else "",
        )
    return entry


def _target_entry(node, kind, written):
    return _entry(kind, [node.catalog, node.db, node.name], written)


def _write_operation(tree, targets):
    """The operation for statements the caller's parser doesn't classify."""
    if isinstance(tree, (exp.Create, exp.Drop)):
        kind = _KINDS.get(str(tree.args.get("kind") or "").upper())
        if kind in ("function", "procedure"):
            return "%s.%s" % (kind, "create" if isinstance(tree, exp.Create) else "drop")
    elif isinstance(tree, exp.Select) and tree.args.get("into") is not None:
        return "table.create"
    elif isinstance(tree, exp.Copy) and targets:
        return "table.insert"
    return None


def _nested_writes(tree):
    """A DML statement buried in a query (`WITH d AS (DELETE …) SELECT …`). The DML inside a
    MERGE's WHEN clauses is part of that statement, not nested."""
    for n in tree.find_all(exp.Insert, exp.Update, exp.Delete, exp.Merge):
        if n is not tree and n.find_ancestor(exp.When) is None:
            return True
    return False


_NAME = r'(?:"[^"]+"|`[^`]+`|\[[^\]]+\]|[A-Za-z_@#$][\w$@#]*)'
_QNAME = _NAME + r"(?:\s*\.\s*" + _NAME + r")*"


def _parts(q):
    return [p.strip('"`[]') for p in re.findall(_NAME, q)]


def _admin(operation, resources, grant=None):
    return {"operation": operation, "resources": resources, "grant": grant}


def _grant_statement(tree):
    op = "grant.grant" if isinstance(tree, exp.Grant) else "grant.revoke"
    kinds = {"TABLE": "table", "VIEW": "view", "SCHEMA": "schema", "DATABASE": "catalog",
             "FUNCTION": "function", "PROCEDURE": "procedure"}
    kind = kinds.get(str(tree.args.get("kind") or "TABLE").upper())
    node = _unwrap(tree.args.get("securable"))
    if kind is None or node is None:
        # An unmodelled securable (e.g. GRANT ... ON SEQUENCE, ON ALL TABLES IN SCHEMA):
        # still report the operation with no resources rather than nothing at all, so the
        # caller's own "an enabled write operation with no identified target" check denies
        # it — silently returning None here would let the whole statement fall through
        # unclassified and unauthorized instead.
        return _admin(op, [])
    privileges, columns = set(), set()
    for p in tree.args.get("privileges") or []:
        privileges.add(str(p.name).upper())
        columns.update(c.name for c in (p.expressions or []) if getattr(c, "name", None))
    grantees = []
    for pr in tree.args.get("principals") or []:
        k = pr.args.get("kind")
        grantees.append("%s:%s" % (str(k).lower(), pr.name) if k else pr.name)
    return _admin(
        op,
        [_target_entry(node, kind, sorted(columns) or None)],
        {
            "privileges": sorted(privileges),
            "grantees": grantees,
            "with_grant_option": bool(tree.args.get("grant_option")),
        },
    )


def _set_entry(item, dialect):
    lhs, rhs = item.this, None
    if isinstance(lhs, exp.EQ):
        lhs, rhs = lhs.this, lhs.expression
    name = (lhs.name if lhs is not None else "") or (lhs.sql(dialect=dialect) if lhs is not None else item.sql(dialect=dialect))
    value = None
    if rhs is not None:
        value = rhs.name if isinstance(rhs, exp.Literal) and rhs.is_string else rhs.sql(dialect=dialect)
    return _entry("session", [name], value=value)


def _command_statement(sql):
    """Statements sqlglot leaves as an opaque Command: role management, SET ROLE / multi-value
    SET / RESET, and CALL. Anything else (CREATE INDEX, EXPLAIN, ...) is not ours."""
    s = sql.strip().rstrip(";").strip()
    names = _NAME + r"(?:\s*,\s*" + _NAME + r")*"

    m = re.match(r"(?is)create\s+(?:or\s+replace\s+)?role\s+(?:if\s+not\s+exists\s+)?(%s)" % _QNAME, s)
    if m:
        return _admin("role.create", [_entry("role", _parts(m.group(1)))])
    m = re.match(r"(?is)drop\s+role\s+(?:if\s+exists\s+)?(%s)" % names, s)
    if m:
        return _admin("role.drop", [_entry("role", [n]) for n in _parts(m.group(1))])
    m = re.match(r"(?is)(grant|revoke)\s+(?!.*\son\s)(?:role\s+)?(%s)\s+(?:to|from)\s+(?:(?:user|role|group)\s+)?(%s)" % (names, names), s)
    if m:
        verb = "grant" if m.group(1).lower() == "grant" else "revoke"
        return _admin(
            "role." + verb,
            [_entry("role", [n]) for n in _parts(m.group(2))],
            {"privileges": [], "grantees": _parts(m.group(3)),
             "with_grant_option": bool(re.search(r"(?i)\bwith\s+admin\s+option\b", s))},
        )
    m = re.match(r"(?is)set\s+(?:(?:session|local)\s+)?role\s+(%s)" % _NAME, s)
    if m:
        return _admin("role.set", [_entry("role", _parts(m.group(1)))])
    # `RESET ALL` resets every session setting, not one named "ALL" — modelling it as
    # session.set on a resource literally called "ALL" would let a policy that denies
    # session.set on a specific setting (e.g. search_path) be bypassed by resetting
    # everything at once under a name no real policy would think to guard. Left
    # unrecognized (like any other statement this function doesn't model) rather than
    # authorized under a name that doesn't actually describe what it does.
    m = re.match(r"(?is)reset\s+(?!all\b)(%s)" % _NAME, s)
    if m:
        return _admin("session.set", [_entry("session", _parts(m.group(1)))])
    m = re.match(r"(?is)set\s+(?:(?:session|local|global)\s+)?(%s)\s*(?:=|\s+to\s+)\s*(.+)$" % _NAME, s)
    if m:
        return _admin("session.set", [_entry("session", _parts(m.group(1)), value=m.group(2).strip())])
    m = re.match(r"(?is)call\s+(%s)\s*\(" % _QNAME, s)
    if m:
        return _admin("procedure.call", [_entry("procedure", _parts(m.group(1)))])
    # A GRANT/REVOKE form sqlglot couldn't parse into a Grant/Revoke node at all (e.g.
    # `... ON ALL TABLES IN SCHEMA ...`, `REVOKE ADMIN OPTION FOR ...`) and none of the
    # role-specific patterns above matched either: still report the operation with no
    # resources, the same reasoning as _grant_statement's own unmodelled-securable case —
    # an enabled grant.grant/grant.revoke with no identified target denies instead of this
    # statement going completely unclassified and unauthorized.
    m = re.match(r"(?is)(grant|revoke)\b", s)
    if m:
        return _admin("grant.grant" if m.group(1).lower() == "grant" else "grant.revoke", [])
    return None


def _admin_statement(tree, sql, dialect):
    """(operation, resources, grant) for statements that read no tables and name administrative
    objects: GRANT/REVOKE, SET/USE/ALTER SESSION, roles, CALL. None for everything else."""
    if isinstance(tree, (exp.Grant, exp.Revoke)):
        return _grant_statement(tree)
    if isinstance(tree, exp.Set):
        return _admin("session.set", [_set_entry(i, dialect) for i in tree.expressions])
    if isinstance(tree, exp.Alter) and str(tree.args.get("kind") or "").upper() == "SESSION":
        items = [i for a in tree.args.get("actions") or [] for i in a.expressions if isinstance(i, exp.SetItem)]
        return _admin("session.set", [_set_entry(i, dialect) for i in items]) if items else None
    if isinstance(tree, exp.Use):
        kind = str(tree.args.get("kind") or "").upper()
        node = _unwrap(tree.this)
        if node is None:
            return None
        parts = [node.catalog, node.db, node.name]
        if kind in ("", "SCHEMA", "DATABASE", "CATALOG"):
            return _admin("session.use", [_entry("catalog" if kind in ("DATABASE", "CATALOG") else "schema", parts)])
        if kind == "ROLE":
            return _admin("role.set", [_entry("role", [node.name])])
        return _admin("session.set", [_entry("session", [kind.lower()], value=node.name)])
    if isinstance(tree, exp.Command):
        return _command_statement(sql)
    return None


def extract_resources(sql, dialect, schema_json):
    schema = json.loads(schema_json) if schema_json else {}
    # A parse failure must surface as an error, never as "no tables": the backend engine may
    # accept SQL that sqlglot rejects, so an empty result here would let it through unchecked.
    try:
        tree = sqlglot.parse_one(sql, dialect=dialect or None)
    except Exception as e:
        return json.dumps({"error": str(e)})

    admin = _admin_statement(tree, sql, dialect)
    if admin is not None:
        return json.dumps({
            "resources": admin["resources"],
            "embeds_reads": False,
            "replaces": False,
            "write_operation": admin["operation"],
            "grant": admin["grant"],
            "nested_writes": False,
        })

    ctes = _cte_names(tree)

    # Best-effort column attribution: qualify when we have a schema.
    qualified = None
    if schema:
        try:
            qualified = qualify(tree.copy(), schema=schema, dialect=dialect or None)
        except Exception:
            qualified = None

    # Build the table listing from whichever tree we'll also read column
    # attribution from (`qualified` when we have one, else the raw `tree`).
    # qualify() renormalizes identifier casing per-dialect (e.g. `Orders` ->
    # `orders` on Trino, `orders` -> `ORDERS` on Snowflake) and can rewrite an
    # unaliased table's synthesized alias too — keying the table listing off a
    # *different* tree than the alias map risks the two keys never matching,
    # which would silently attribute zero columns to a real, matched table.
    src = qualified if qualified is not None else tree

    targets, embeds_reads, replaces = _write_targets(src)
    target_ids = {id(n) for n, _ in targets}
    # Read the written columns from the statement as parsed: qualify() (run when a catalog is
    # configured) can rewrite a column list away, e.g. `COPY t (a, b) FROM …`.
    written = _written_columns(tree)

    # Write targets are reported separately below, so a read of the same table
    # (`INSERT INTO t SELECT ... FROM t`) stays its own entry and is still policy-checked.
    per_table = {}
    order = []
    for t in src.find_all(exp.Table):
        if id(t) in target_ids or not t.name or t.name.startswith("@"):
            continue
        if not t.catalog and not t.db and t.name.lower() in ctes:
            continue
        key = _table_key(t)
        if key not in per_table:
            per_table[key] = {
                "catalog": t.catalog or None,
                "schema": t.db or None,
                "table": t.name,
                "columns": set(),
                "all": False,
            }
            order.append(key)

    # star -> all columns
    for star in src.find_all(exp.Star):
        for e in per_table.values():
            e["all"] = True
    # attribute columns to their table
    if qualified is not None:
        alias_to_table = {}
        for t in qualified.find_all(exp.Table):
            a = (t.alias or t.name)
            if a:
                alias_to_table[a] = _table_key(t)
        for col in qualified.find_all(exp.Column):
            tbl = col.table
            if tbl and tbl in alias_to_table:
                k = alias_to_table[tbl]
                if k in per_table:
                    per_table[k]["columns"].add(col.name)
    else:
        # no schema: we cannot safely attribute bare columns -> all
        for e in per_table.values():
            e["all"] = True

    out = []
    for key in order:
        e = per_table[key]
        out.append({
            "catalog": e["catalog"],
            "schema": e["schema"],
            "table": e["table"],
            "columns": None if e["all"] else sorted(e["columns"]),
            "target": False,
            "kind": "table",
        })
    # A write target reports what the statement writes, not what its WHERE reads.
    for node, kind in targets:
        out.append(_target_entry(node, kind, written))
    return json.dumps({
        "resources": out,
        "embeds_reads": embeds_reads,
        "replaces": replaces,
        "write_operation": _write_operation(src, targets),
        "grant": None,
        "nested_writes": _nested_writes(src),
    })


def apply_write_filters(sql, dialect, filters_json):
    filters = json.loads(filters_json)
    tree = sqlglot.parse_one(sql, dialect=dialect or None)
    if not isinstance(tree, (exp.Update, exp.Delete, exp.Merge)):
        raise ValueError(
            "row filters can only scope UPDATE, DELETE and MERGE, got %s" % type(tree).__name__
        )
    targets, _, _ = _write_targets(tree)
    if len(targets) != 1:
        raise ValueError("cannot identify the statement's write target")
    ref = targets[0][0].alias or targets[0][0].name

    conditions = []
    for f in filters:
        cond = sqlglot.parse_one(f, dialect=dialect or None)
        # Qualify the filter's own columns with the target so they can't become ambiguous
        # against a joined table. Columns inside a subquery belong to that subquery.
        for col in list(cond.find_all(exp.Column)):
            if not col.table and col.find_ancestor(exp.Select) is None:
                col.set("table", exp.to_identifier(ref))
        conditions.append(cond)

    if isinstance(tree, exp.Merge):
        # Only WHEN branches that act on a target row that already exists can be scoped this
        # way: WHEN MATCHED (matched=True) and WHEN NOT MATCHED BY SOURCE (matched=False,
        # source=True) both UPDATE/DELETE an existing row. WHEN NOT MATCHED [BY TARGET]
        # (matched=False, source=False) INSERTs a brand new row, which has no existing row to
        # filter against — the same WITH-CHECK gap plain INSERT has, left alone here.
        for when in tree.args["whens"].expressions:
            if not (when.args.get("matched") or when.args.get("source")):
                continue
            extra = exp.and_(*[c.copy() for c in conditions])
            existing = when.args.get("condition")
            when.set("condition", exp.and_(existing, extra) if existing else extra)
        return tree.sql(dialect=dialect or None)

    return tree.where(*conditions, append=True, copy=False).sql(dialect=dialect or None)


def rewrite_table_scans(sql, dialect, schema_json, policies_json):
    schema = json.loads(schema_json) if schema_json else {}
    policies = json.loads(policies_json)
    tree = sqlglot.parse_one(sql, dialect=dialect or None)
    ctes = _cte_names(tree)
    # The write target is not a scan site: swapping it for a filtered subquery would
    # produce invalid SQL (`UPDATE (SELECT ...) AS t ...`) and would not restrict anything.
    targets, _, _ = _write_targets(tree)
    target_ids = {id(n) for n, _ in targets}

    # Index policies (lowercased). A qualified policy is only reachable by an unqualified scan
    # through `qualified_by_bare`; an explicitly qualified scan never resolves to another
    # schema's policy.
    exact = {}
    qualified_by_bare = {}
    for p in policies:
        key = p["table"].lower()
        exact[key] = p
        if "." in key:
            qualified_by_bare.setdefault(key.rsplit(".", 1)[-1], []).append(p)

    def find_policy(t):
        name = t.name.lower()
        if t.db:
            # explicit schema: exact `schema.table`, else a policy written for the bare name
            return exact.get(_qualified(t).lower()) or exact.get(name)
        if name in exact:
            return exact[name]
        cands = qualified_by_bare.get(name, [])
        if len(cands) > 1:
            # cannot tell which schema the scan resolves to -> fail closed
            raise ValueError(
                "ambiguous policy for unqualified table %r: %s"
                % (t.name, ", ".join(sorted(c["table"] for c in cands)))
            )
        return cands[0] if cands else None

    def _star_selects_this_scan(table_node):
        p = table_node.parent
        while p is not None:
            if isinstance(p, exp.Select):
                for e in p.expressions:
                    if isinstance(e, exp.Star):
                        return True
                    if isinstance(e, exp.Column) and e.name == "*":
                        # `a.*` expands only `a`; other scans in the same SELECT are not starred.
                        qualifier = (e.table or "").lower()
                        if not qualifier or qualifier in (
                            (table_node.alias or "").lower(),
                            (table_node.name or "").lower(),
                        ):
                            return True
                return False
            p = p.parent
        return False

    def _enclosing_select(table_node):
        p = table_node.parent
        while p is not None:
            if isinstance(p, exp.Select):
                return p
            p = p.parent
        return None

    def _in_select_scope(node, select):
        p = node.parent
        while p is not None:
            if isinstance(p, exp.Select):
                return p is select
            p = p.parent
        return False

    def col_list_for(policy_table, node, masks):
        # explicit schema columns win; else named references in the *enclosing*
        # SELECT (plus every masked column) are enough for a projection.
        # Unqualified names are taken only from that SELECT so a join's
        # `IN (SELECT id FROM t)` still projects `id` on the inner scan.
        # SELECT * still needs schema — we cannot invent the rest of the table.
        cand = [policy_table.lower(), policy_table.rsplit(".", 1)[-1].lower()]
        for tname, cols in schema.items():
            if tname.lower() in cand or tname.rsplit(".", 1)[-1].lower() in cand:
                return list(cols)
        if _star_selects_this_scan(node):
            return None
        alias = (node.alias or node.name or "").lower()
        names = []
        seen = set()

        def add(n):
            if not n:
                return
            k = n.lower()
            if k not in seen:
                seen.add(k)
                names.append(n)

        for mcol in masks:
            add(mcol)
        scope = _enclosing_select(node)
        src = scope if scope is not None else tree
        sole_in_scope = False
        if scope is not None:
            tables_here = [
                tbl
                for tbl in scope.find_all(exp.Table)
                if tbl.name
                and not (not tbl.catalog and not tbl.db and tbl.name.lower() in ctes)
                and _in_select_scope(tbl, scope)
            ]
            sole_in_scope = len(tables_here) == 1
        for col in src.find_all(exp.Column):
            if not col.name or col.name == "*":
                continue
            if scope is not None and not _in_select_scope(col, scope):
                continue
            tbl = (col.table or "").lower()
            if tbl:
                if tbl == alias or tbl in cand:
                    add(col.name)
            elif sole_in_scope:
                add(col.name)
        return names if names else None

    replaced = 0
    for t in list(tree.find_all(exp.Table)):
        if id(t) in target_ids or not t.name:
            continue
        if not t.catalog and not t.db and t.name.lower() in ctes:
            continue
        policy = find_policy(t)
        if policy is None:
            continue

        alias = t.alias or t.name

        masks = {m["column"].lower(): m["expr"] for m in policy["masked_columns"]}
        filters = [f for f in policy["row_filters"] if f and f.strip()]

        # projection
        if masks:
            cols = col_list_for(policy["table"], t, masks)
            if not cols:
                raise ValueError("cannot enumerate columns for masked table %r" % policy["table"])
            selects = []
            for c in cols:
                if c.lower() in masks:
                    expr = sqlglot.parse_one(masks[c.lower()], dialect=dialect or None)
                    selects.append(exp.alias_(expr, c))
                else:
                    selects.append(exp.column(c))
        else:
            selects = [exp.Star()]

        inner = exp.select(*selects).from_(exp.table_(t.name, db=t.db, catalog=t.catalog))
        if filters:
            cond = None
            for f in filters:
                fe = sqlglot.parse_one(f, dialect=dialect or None)
                cond = fe if cond is None else exp.and_(cond, fe)
            inner = inner.where(cond)

        sub = exp.Subquery(this=inner, alias=exp.TableAlias(this=exp.to_identifier(alias)))
        t.replace(sub)
        replaced += 1

    return tree.sql(dialect=dialect or None)
