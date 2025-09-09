package builders

import (
	"anyFilters/schemas"
	"anyFilters/types"
	"anyFilters/utils"
	"fmt"
	"strings"
)

func BuildSelect(spec types.QuerySpec, sch schemas.Schema) (string, []any, error) {
	tab, ok := sch.Tables[spec.Table]
	if !ok {
		return "", nil, fmt.Errorf("table %q not allowed", spec.Table)
	}

	// SELECT
	cols := make([]string, 0, len(spec.Select))
	if len(spec.Select) == 0 {
		for name := range tab.Columns {
			cols = append(cols, utils.QuoteIdentPG(name))
		}
	} else {
		for _, c := range spec.Select {
			if _, ok := tab.Columns[c]; !ok {
				return "", nil, fmt.Errorf("unknown column %q", c)
			}
			cols = append(cols, utils.QuoteIdentPG(c))
		}
	}

	var sb strings.Builder
	args := make([]any, 0, 16)
	i := 1
	sb.WriteString("SELECT ")
	sb.WriteString(strings.Join(cols, ", "))
	sb.WriteString(" FROM ")
	sb.WriteString(utils.QuoteIdentPG(tab.Name))
	sb.WriteString(" WHERE 1=1")

	// WHERE
	for _, w := range spec.Where {
		col, ok := tab.Columns[w.Field]
		if !ok {
			return "", nil, fmt.Errorf("unknown where column %q", w.Field)
		}
		switch w.Op {
		case types.OpEq:
			sb.WriteString(" AND ")
			sb.WriteString(utils.QuoteIdentPG(col.Name))
			sb.WriteString(fmt.Sprintf(" = $%d", i))
			args = append(args, w.Value)
			i++
		case types.OpGte:
			sb.WriteString(" AND ")
			sb.WriteString(utils.QuoteIdentPG(col.Name))
			sb.WriteString(fmt.Sprintf(" >= $%d", i))
			args = append(args, w.Value)
			i++
		case types.OpLte:
			sb.WriteString(" AND ")
			sb.WriteString(utils.QuoteIdentPG(col.Name))
			sb.WriteString(fmt.Sprintf(" <= $%d", i))
			args = append(args, w.Value)
			i++
		case types.OpIn:
			slice, ok := toSlice(w.Value)
			if !ok || len(slice) == 0 {
				sb.WriteString(" AND 1=0")
				continue
			}
			sb.WriteString(" AND ")
			sb.WriteString(utils.QuoteIdentPG(col.Name))
			sb.WriteString(" IN (")
			for k := range slice {
				if k > 0 {
					sb.WriteByte(',')
				}
				sb.WriteString(fmt.Sprintf("$%d", i))
				args = append(args, slice[k])
				i++
			}
			sb.WriteString(")")
		case types.OpLikePrefix, types.OpILikePrefix:
			if col.Type != types.ColText {
				return "", nil, fmt.Errorf("LIKE prefix on non-text %q", col.Name)
			}
			sb.WriteString(" AND ")
			sb.WriteString(utils.QuoteIdentPG(col.Name))
			if w.Op == types.OpILikePrefix {
				sb.WriteString(fmt.Sprintf(" ILIKE $%d", i))
			} else {
				sb.WriteString(fmt.Sprintf(" LIKE $%d", i))
			}
			args = append(args, fmt.Sprintf("%v%%", w.Value))
			i++
		default:
			return "", nil, fmt.Errorf("unsupported op %v", w.Op)
		}
	}

	// ORDER BY
	if len(spec.Sort) > 0 {
		sb.WriteString(" ORDER BY ")
		for k, s := range spec.Sort {
			if _, ok := tab.Columns[s.Field]; !ok {
				return "", nil, fmt.Errorf("unknown sort %q", s.Field)
			}
			if k > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(utils.QuoteIdentPG(s.Field))
			if s.Dir == types.Desc {
				sb.WriteString(" DESC")
			} else {
				sb.WriteString(" ASC")
			}
		}
		// tie-break по PK
		if tab.PrimaryKey != "" && spec.Sort[len(spec.Sort)-1].Field != tab.PrimaryKey {
			sb.WriteString(", ")
			sb.WriteString(utils.QuoteIdentPG(tab.PrimaryKey))
			sb.WriteString(" ASC")
		}
	}

	// LIMIT/OFFSET
	if spec.Page != nil {
		lim := spec.Page.Limit
		if lim <= 0 || lim > 1000 {
			lim = 100
		}
		sb.WriteString(fmt.Sprintf(" LIMIT $%d", i))
		args = append(args, lim)
		i++
		sb.WriteString(fmt.Sprintf(" OFFSET $%d", i))
		args = append(args, spec.Page.Offset)
		i++
	}

	return sb.String(), args, nil
}

func BuildCount(spec types.QuerySpec, sch schemas.Schema) (string, []any, error) {
	// тот же WHERE, без ORDER/LIMIT, SELECT count(*)
	spec2 := spec
	spec2.Select = nil
	spec2.Sort = nil
	spec2.Page = nil
	sql, args, err := BuildSelect(spec2, sch)
	if err != nil {
		return "", nil, err
	}
	return "SELECT count(*) FROM (" + sql + ") t", args, nil
}

func toSlice(v any) ([]any, bool) {
	switch s := v.(type) {
	case []any:
		return s, true
	case []int:
		out := make([]any, len(s))
		for i := range s {
			out[i] = s[i]
		}
		return out, true
	case []int64:
		out := make([]any, len(s))
		for i := range s {
			out[i] = s[i]
		}
		return out, true
	case []string:
		out := make([]any, len(s))
		for i := range s {
			out[i] = s[i]
		}
		return out, true
	default:
		return nil, false
	}
}
