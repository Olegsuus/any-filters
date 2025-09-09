package anylize

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
)

type DBTX interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func ExplainAnalyze(ctx context.Context, db DBTX, sqlStr string, args ...any) (plan string, ms float64, err error) {
	q := "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) " + sqlStr
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return "", 0, err
	}
	defer rows.Close()
	var b strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return "", 0, err
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	plan = b.String()
	ms = parseExec(plan)
	return plan, ms, rows.Err()
}

var execRe = regexp.MustCompile(`Execution Time:\s+([0-9.]+)\s+ms`)

func parseExec(plan string) float64 {
	m := execRe.FindStringSubmatch(plan)
	if len(m) == 2 {
		var f float64
		fmt.Sscanf(m[1], "%f", &f)
		return f
	}
	return 0
}
