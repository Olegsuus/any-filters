package schemas

import (
	"anyFilters/types"
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type Column struct {
	Name string
	Type types.ColType
}
type Table struct {
	Name       string
	Columns    map[string]Column
	PrimaryKey string
}
type Schema struct{ Tables map[string]Table }

func LoadSchema(ctx context.Context, db *sql.DB, tables []string) (Schema, error) {
	if len(tables) == 0 {
		return Schema{}, fmt.Errorf("no tables")
	}
	args := make([]any, 0, len(tables))
	ph := make([]string, 0, len(tables))
	for i, t := range tables {
		args = append(args, t)
		ph = append(ph, fmt.Sprintf("$%d", i+1))
	}

	q := fmt.Sprintf(`
SELECT c.table_name, c.column_name, c.data_type,
       COALESCE(tc.constraint_type='PRIMARY KEY',false) AS is_pk
FROM information_schema.columns c
LEFT JOIN information_schema.key_column_usage k
  ON k.table_name=c.table_name AND k.column_name=c.column_name
LEFT JOIN information_schema.table_constraints tc
  ON tc.table_name=k.table_name AND tc.constraint_name=k.constraint_name
WHERE c.table_schema='public' AND c.table_name IN (%s)
ORDER BY c.table_name,c.ordinal_position;`, strings.Join(ph, ","))

	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return Schema{}, err
	}
	defer rows.Close()

	s := Schema{Tables: map[string]Table{}}
	for rows.Next() {
		var tname, cname, dtype string
		var isPK bool
		if err := rows.Scan(&tname, &cname, &dtype, &isPK); err != nil {
			return Schema{}, err
		}
		tab := s.Tables[tname]
		if tab.Name == "" {
			tab = Table{Name: tname, Columns: map[string]Column{}}
		}
		tab.Columns[cname] = Column{Name: cname, Type: mapDataType(dtype)}
		if isPK && tab.PrimaryKey == "" {
			tab.PrimaryKey = cname
		}
		s.Tables[tname] = tab
	}
	return s, rows.Err()
}

func mapDataType(d string) types.ColType {
	d = strings.ToLower(d)
	switch {
	case strings.Contains(d, "char"), strings.Contains(d, "text"), strings.Contains(d, "citext"):
		return types.ColText
	case strings.Contains(d, "int"), strings.Contains(d, "numeric"), strings.Contains(d, "decimal"), strings.Contains(d, "real"), strings.Contains(d, "double"):
		return types.ColNumeric
	case strings.Contains(d, "bool"):
		return types.ColBool
	case strings.Contains(d, "time"), strings.Contains(d, "date"):
		return types.ColTime
	case strings.Contains(d, "uuid"):
		return types.ColUUID
	case strings.Contains(d, "json"):
		return types.ColJSON
	default:
		return types.ColUnknown
	}
}
