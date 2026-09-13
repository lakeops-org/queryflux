# Demo policy for examples/with-opa.
#
# Input (QueryFlux → POST /v1/data/queryflux/access):
#   input.identity.user / .groups / .roles / .attributes
#   input.action.operation          e.g. "table.select"
#   input.action.resources[].table  bare or schema-qualified name
#   input.context.clusterGroup / engine / queryId / sessionParams
#
# Output: { "resources": [ { table, allow, reason?, rowFilters?, columnMasks? } ] }
#
# alice (group engineers): full rows, unmasked SSN, payroll allowed
# bob   (group analysts):  EU customers only, SSN SHOW_LAST_4, payroll denied

package queryflux.access

import rego.v1

resources := [d |
	some r in input.action.resources
	d := table_decision(r)
]

bare(name) := n if {
	parts := split(name, ".")
	n := parts[count(parts) - 1]
}

is_engineer if "engineers" in input.identity.groups

is_analyst if "analysts" in input.identity.groups

# --- mutually exclusive complete definitions ---

table_decision(r) := {
	"table": r.table,
	"allow": true,
} if {
	is_engineer
	bare(r.table) in {"customers", "payroll"}
}

table_decision(r) := {
	"table": r.table,
	"allow": true,
	"rowFilters": [{"expression": "region = 'EU'"}],
	"columnMasks": [{"column": "ssn", "type": "SHOW_LAST_4"}],
} if {
	is_analyst
	bare(r.table) == "customers"
}

table_decision(r) := {
	"table": r.table,
	"allow": false,
	"reason": sprintf("%s cannot read payroll", [input.identity.user]),
} if {
	not is_engineer
	bare(r.table) == "payroll"
}

# Keep this disjoint from the payroll rule above — two matching complete
# definitions for the same `r` is an OPA eval error.
table_decision(r) := {
	"table": r.table,
	"allow": false,
	"reason": sprintf("table %q is not granted", [r.table]),
} if {
	not known_grant(r)
	bare(r.table) != "payroll"
}

known_grant(r) if {
	is_engineer
	bare(r.table) in {"customers", "payroll"}
}

known_grant(r) if {
	is_analyst
	bare(r.table) == "customers"
}
