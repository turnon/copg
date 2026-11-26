package lib

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// statementDescriber is implemented by *pgx.Conn and pgx.Tx.
type statementDescriber interface {
	Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error)
}

// prepareSelect resolves the export's SELECT list together with the column
// metadata the binary decoder needs.
//
// Without explicit expressions it exports every live column of the relation,
// exactly as pg_attribute reports it. With explicit expressions the result
// types come from the query's result descriptor instead, because an expression
// has no pg_attribute entry.
//
// filter is the caller's optional row predicate; it is not part of the SELECT
// list but is prepared as part of the probe so that an invalid predicate is
// reported once during planning instead of in every shard.
func prepareSelect(ctx context.Context, tx pgx.Tx, table, list, filter string) (string, []ColumnMeta, error) {
	qtable := quoteIdentifier(table)
	probeFilter := "false"
	if filter = strings.TrimSpace(filter); filter != "" {
		probeFilter = fmt.Sprintf("false AND (%s)", filter)
		// Validate first so a bad predicate is named as such instead of
		// surfacing as a generic prepare error from the probe below.
		if err := validateFilter(ctx, tx, qtable, probeFilter); err != nil {
			return "", nil, err
		}
	}

	if strings.TrimSpace(list) == "" {
		cols, err := fetchColumnMeta(ctx, tx, table)
		if err != nil {
			return "", nil, err
		}
		names := make([]string, len(cols))
		for i, c := range cols {
			names[i] = quoteIdentifier(c.Name)
		}
		return strings.Join(names, ", "), cols, nil
	}

	// The zero-row probe is never executed for data, only prepared and
	// explained: its result descriptor describes the export.
	probe := fmt.Sprintf("SELECT %s FROM %s WHERE %s", list, qtable, probeFilter)

	var cols []ColumnMeta
	err := inSavepoint(ctx, tx, func(p pgx.Tx) error {
		var err error
		if cols, err = describeSelect(ctx, p, probe); err != nil {
			return err
		}
		return checkUniqueNames(cols)
	})
	if err != nil {
		return "", nil, diagnoseSelect(ctx, tx, qtable, list, err)
	}

	// Fast path for the common case: one plan for the whole column list.
	var node string
	err = inSavepoint(ctx, tx, func(p pgx.Tx) error {
		var err error
		node, err = topNode(ctx, p, probe)
		return err
	})
	if err != nil {
		return "", nil, err
	}
	if kind, bad := nonRowLevel(node); bad {
		return "", nil, diagnoseSelect(ctx, tx, qtable, list, notRowLevelError(kind))
	}

	if err := fillTypeNames(ctx, tx, cols); err != nil {
		return "", nil, err
	}
	return list, cols, nil
}

// validateFilter prepares a zero-row query containing filter so PostgreSQL
// parses and plans it. Preparing is enough to catch unknown columns, type
// errors and aggregates (which WHERE forbids anyway) without scanning rows.
func validateFilter(ctx context.Context, tx pgx.Tx, qtable, probeFilter string) error {
	probe := fmt.Sprintf("SELECT 1 FROM %s WHERE %s", qtable, probeFilter)
	return inSavepoint(ctx, tx, func(p pgx.Tx) error {
		if _, err := p.Prepare(ctx, "", probe); err != nil {
			return fmt.Errorf("copg: where clause: %w", err)
		}
		return nil
	})
}

// describeSelect returns the result columns of sql without running it.
func describeSelect(ctx context.Context, q statementDescriber, sql string) ([]ColumnMeta, error) {
	sd, err := q.Prepare(ctx, "", sql)
	if err != nil {
		return nil, err
	}
	if len(sd.Fields) == 0 {
		return nil, errors.New("copg: export query has no result columns")
	}
	metas := make([]ColumnMeta, len(sd.Fields))
	for i, f := range sd.Fields {
		metas[i] = ColumnMeta{Name: f.Name, OID: f.DataTypeOID}
	}
	return metas, nil
}

// inSavepoint runs fn inside a savepoint. A server error aborts the whole
// transaction, and a savepoint cannot be created once that happened, so the
// savepoint has to exist before the risky statement. The export never writes
// anything, which makes rolling the savepoint back free: it only restores the
// snapshot transaction to a usable state.
func inSavepoint(ctx context.Context, tx pgx.Tx, fn func(pgx.Tx) error) error {
	sp, err := tx.Begin(ctx)
	if err != nil {
		return err
	}
	ferr := fn(sp)
	if rerr := sp.Rollback(ctx); rerr != nil && ferr == nil {
		return fmt.Errorf("copg: release savepoint: %w", rerr)
	}
	return ferr
}

// diagnoseSelect probes the expressions one at a time to name the invalid one,
// or the one that aggregates. It returns cause when no single expression can be
// blamed.
func diagnoseSelect(ctx context.Context, tx pgx.Tx, qtable, list string, cause error) error {
	exprs, err := splitSelectList(list)
	if err != nil {
		// Blaming the whole list beats a follow-up error about splitting it.
		return cause
	}
	for i, expr := range exprs {
		probe := fmt.Sprintf("SELECT %s FROM %s WHERE false", expr, qtable)
		var found error
		err := inSavepoint(ctx, tx, func(p pgx.Tx) error {
			if _, err := p.Prepare(ctx, "", probe); err != nil {
				found = fmt.Errorf("copg: column %d (%s): %w", i+1, expr, err)
				return err
			}
			node, err := topNode(ctx, p, probe)
			if err != nil {
				return err
			}
			if kind, bad := nonRowLevel(node); bad {
				found = fmt.Errorf("copg: column %d (%s) uses %s; shards scan disjoint ctid ranges, "+
					"so it would be evaluated once per shard instead of once per table. "+
					"Export the rows and %s afterwards", i+1, expr, kind, finishHint(kind))
			}
			return nil
		})
		if found != nil {
			return found
		}
		if err != nil {
			// Diagnosis itself failed (e.g. no savepoint possible); the
			// original error is more useful than a follow-up failure.
			return cause
		}
	}
	return cause
}

func notRowLevelError(kind string) error {
	return fmt.Errorf("copg: the export query uses %s; shards scan disjoint ctid ranges, "+
		"so it would be evaluated once per shard instead of once per table. "+
		"Export the rows and %s afterwards", kind, finishHint(kind))
}

func finishHint(kind string) string {
	if strings.Contains(kind, "window") {
		return "apply the window function"
	}
	return "aggregate"
}

// explainPlan is the subset of EXPLAIN (VERBOSE, FORMAT JSON) needed to see
// whether the query aggregates rows.
type explainPlan []struct {
	Plan struct {
		NodeType string `json:"Node Type"`
	} `json:"Plan"`
}

// topNode returns the node type of the query plan's root, e.g. "Result",
// "Aggregate" or "WindowAgg".
func topNode(ctx context.Context, q rowQuerier, probe string) (string, error) {
	var raw []byte
	if err := q.QueryRow(ctx, "EXPLAIN (VERBOSE, FORMAT JSON) "+probe).Scan(&raw); err != nil {
		return "", fmt.Errorf("copg: explain export query: %w", err)
	}
	return parseTopNode(raw)
}

func parseTopNode(raw []byte) (string, error) {
	var plans explainPlan
	if err := json.Unmarshal(raw, &plans); err != nil {
		return "", fmt.Errorf("copg: decode EXPLAIN output: %w", err)
	}
	if len(plans) == 0 {
		return "", errors.New("copg: empty EXPLAIN output")
	}
	return plans[0].Plan.NodeType, nil
}

// nonRowLevel reports whether a root plan node aggregates or windows rows.
// Subquery aggregates appear below the root node (as subplans), so only the top
// is inspected — that is where an aggregate in the SELECT list always lands.
func nonRowLevel(node string) (string, bool) {
	switch {
	case strings.Contains(node, "Window"):
		return "a window function", true
	case strings.Contains(node, "Agg"):
		return "an aggregate function", true
	}
	return "", false
}

// splitSelectList splits a SELECT list on top-level commas. Commas inside
// parentheses, brackets, quoted strings/identifiers or dollar-quoted bodies
// belong to their expression, so "to_char(ts, 'YYYY-MM-DD') AS d" stays one
// entry. It is used for error attribution: the export itself is passed to
// PostgreSQL verbatim.
func splitSelectList(s string) ([]string, error) {
	var exprs []string
	start := 0
	depth := 0
	brackets := 0
	for i := 0; i < len(s); {
		switch c := s[i]; {
		case c == '\'' || c == '"':
			q := c
			i++
			for i < len(s) {
				if s[i] != q {
					i++
					continue
				}
				// A doubled quote is an escaped quote, not the end.
				if i+1 < len(s) && s[i+1] == q {
					i += 2
					continue
				}
				break
			}
			if i >= len(s) {
				return nil, fmt.Errorf("unterminated %s quote", quoteName(q))
			}
			i++ // closing quote
		case c == '$' && isDollarQuoteStart(s[i:]):
			tag := dollarQuoteTag(s[i:])
			end := strings.Index(s[i+len(tag):], tag)
			if end < 0 {
				return nil, fmt.Errorf("unterminated dollar-quoted string %s", tag)
			}
			i += len(tag) + end + len(tag)
		case c == '(':
			depth++
			i++
		case c == ')':
			depth--
			if depth < 0 {
				return nil, errors.New("unbalanced ')' in select list")
			}
			i++
		case c == '[':
			brackets++
			i++
		case c == ']':
			brackets--
			if brackets < 0 {
				return nil, errors.New("unbalanced ']' in select list")
			}
			i++
		case c == ',' && depth == 0 && brackets == 0:
			exprs = append(exprs, s[start:i])
			i++
			start = i
		default:
			i++
		}
	}
	if depth != 0 {
		return nil, errors.New("unbalanced '(' in select list")
	}
	if brackets != 0 {
		return nil, errors.New("unbalanced '[' in select list")
	}
	exprs = append(exprs, s[start:])

	out := exprs[:0]
	for _, expr := range exprs {
		expr = strings.TrimSpace(expr)
		if expr == "" {
			return nil, errors.New("empty column expression")
		}
		out = append(out, expr)
	}
	return out, nil
}

func quoteName(q byte) string {
	if q == '\'' {
		return "string"
	}
	return "identifier"
}

// isDollarQuoteStart reports whether s begins a dollar-quoted string opener
// such as "$$" or "$body$". Positional parameters ("$1") do not qualify.
func isDollarQuoteStart(s string) bool {
	return dollarQuoteTag(s) != ""
}

// dollarQuoteTag returns the opening delimiter of the dollar-quoted string at
// the start of s, or "" when s does not start one.
func dollarQuoteTag(s string) string {
	if s == "" || s[0] != '$' {
		return ""
	}
	i := 1
	if i < len(s) && s[i] == '$' {
		return "$$"
	}
	if i >= len(s) || !(s[i] == '_' || isLetter(s[i])) {
		return ""
	}
	for i < len(s) && (s[i] == '_' || isLetter(s[i]) || isDigit(s[i])) {
		i++
	}
	if i < len(s) && s[i] == '$' {
		return s[:i+1]
	}
	return ""
}

func isLetter(c byte) bool { return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' }
func isDigit(c byte) bool  { return c >= '0' && c <= '9' }

// checkUniqueNames rejects duplicate output names: consumers such as
// JSONLWriter key rows by name, so two columns with the same name would be
// indistinguishable.
func checkUniqueNames(cols []ColumnMeta) error {
	seen := make(map[string]int, len(cols))
	for i, c := range cols {
		if j, dup := seen[c.Name]; dup {
			return fmt.Errorf("copg: columns %d and %d both produce the output name %q; add an AS alias to tell them apart", j+1, i+1, c.Name)
		}
		seen[c.Name] = i
	}
	return nil
}

// fillTypeNames annotates the columns with pg_type.typname for documentation
// and error messages. Prepared result descriptors only carry the OID.
func fillTypeNames(ctx context.Context, q rowsQuerier, cols []ColumnMeta) error {
	if len(cols) == 0 {
		return nil
	}
	oids := make([]uint32, 0, len(cols))
	seen := make(map[uint32]bool, len(cols))
	for _, c := range cols {
		if !seen[c.OID] {
			seen[c.OID] = true
			oids = append(oids, c.OID)
		}
	}

	rows, err := q.Query(ctx, "SELECT oid, typname FROM pg_type WHERE oid = ANY($1::oid[])", oids)
	if err != nil {
		return fmt.Errorf("copg: query pg_type: %w", err)
	}
	defer rows.Close()

	names := make(map[uint32]string, len(oids))
	for rows.Next() {
		var oid uint32
		var name string
		if err := rows.Scan(&oid, &name); err != nil {
			return fmt.Errorf("copg: scan pg_type: %w", err)
		}
		names[oid] = name
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("copg: read pg_type: %w", err)
	}
	for i := range cols {
		cols[i].TypeName = names[cols[i].OID]
	}
	return nil
}
