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
func prepareSelect(ctx context.Context, tx pgx.Tx, table string, exprs []string) (string, []ColumnMeta, error) {
	qtable := quoteIdentifier(table)

	if len(exprs) == 0 {
		cols, err := fetchColumnMeta(ctx, tx, table)
		if err != nil {
			return "", nil, err
		}
		list := make([]string, len(cols))
		for i, c := range cols {
			list[i] = quoteIdentifier(c.Name)
		}
		return strings.Join(list, ", "), cols, nil
	}

	list := strings.Join(exprs, ", ")
	// The zero-row probe is never executed for data, only prepared and
	// explained: its result descriptor describes the export.
	probe := fmt.Sprintf("SELECT %s FROM %s WHERE false", list, qtable)

	var cols []ColumnMeta
	err := inSavepoint(ctx, tx, func(p pgx.Tx) error {
		var err error
		if cols, err = describeSelect(ctx, p, probe); err != nil {
			return err
		}
		return checkUniqueNames(cols)
	})
	if err != nil {
		return "", nil, diagnoseSelect(ctx, tx, qtable, exprs, err)
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
		return "", nil, diagnoseSelect(ctx, tx, qtable, exprs, notRowLevelError(kind))
	}

	if err := fillTypeNames(ctx, tx, cols); err != nil {
		return "", nil, err
	}
	return list, cols, nil
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
func diagnoseSelect(ctx context.Context, tx pgx.Tx, qtable string, exprs []string, cause error) error {
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
