package builders

import (
	"fmt"
	"github.com/Olegsuus/any-filters/schemas"
	"github.com/Olegsuus/any-filters/types"
	"github.com/Olegsuus/any-filters/utils"
	"strings"
)

// BuildSelect строит SELECT для PostgreSQL, валидируя по Schema.
// Возвращает готовые sql/args с плейсхолдерами $1..$n.
func BuildSelect(spec types.QuerySpec, sch schemas.Schema) (string, []any, error) {
	tab, ok := sch.Tables[spec.Table]
	if !ok {
		return "", nil, fmt.Errorf("table %q not allowed", spec.Table)
	}

	// SELECT
	var cols []string
	if len(spec.Select) == 0 {
		// по умолчанию — все колонки (в порядке из schemas)
		for name := range tab.Columns {
			cols = append(cols, utils.QuoteIdentPG(name))
		}
	} else {
		for _, c := range spec.Select {
			if _, ok := tab.Columns[c]; !ok {
				return "", nil, fmt.Errorf("unknown column %q for table %q", c, spec.Table)
			}
			cols = append(cols, utils.QuoteIdentPG(c))
		}
	}

	var sb strings.Builder
	args := make([]any, 0, 16)
	argn := 1

	sb.WriteString("SELECT ")
	sb.WriteString(strings.Join(cols, ", "))
	sb.WriteString(" FROM ")
	sb.WriteString(utils.QuoteIdentPG(tab.Name))
	sb.WriteString(" WHERE 1=1")

	// WHERE
	for _, w := range spec.Where {
		col, ok := tab.Columns[w.Field]
		if !ok {
			return "", nil, fmt.Errorf("unknown column in WHERE: %q", w.Field)
		}
		switch w.Op {
		case types.OpEq:
			sb.WriteString(" AND ")
			sb.WriteString(utils.QuoteIdentPG(col.Name))
			sb.WriteString(fmt.Sprintf(" = $%d", argn))
			args = append(args, w.Value)
			argn++

		case types.OpGte:
			sb.WriteString(" AND ")
			sb.WriteString(utils.QuoteIdentPG(col.Name))
			sb.WriteString(fmt.Sprintf(" >= $%d", argn))
			args = append(args, w.Value)
			argn++

		case types.OpLte:
			sb.WriteString(" AND ")
			sb.WriteString(utils.QuoteIdentPG(col.Name))
			sb.WriteString(fmt.Sprintf(" <= $%d", argn))
			args = append(args, w.Value)
			argn++

		case types.OpIn:
			// ожидаем slice; развернём в ($n,$n+1,...)
			slice, ok := toSlice(w.Value)
			if !ok || len(slice) == 0 {
				// пустой IN — делаем заведомо ложное условие
				sb.WriteString(" AND 1=0")
				continue
			}
			sb.WriteString(" AND ")
			sb.WriteString(utils.QuoteIdentPG(col.Name))
			sb.WriteString(" IN (")
			for i := range slice {
				if i > 0 {
					sb.WriteByte(',')
				}
				sb.WriteString(fmt.Sprintf("$%d", argn))
				args = append(args, slice[i])
				argn++
			}
			sb.WriteString(")")

		case types.OpLikePrefix, types.OpILikePrefix:
			if col.Type != types.ColText {
				return "", nil, fmt.Errorf("LIKE prefix on non-text column %q", col.Name)
			}
			sb.WriteString(" AND ")
			sb.WriteString(utils.QuoteIdentPG(col.Name))
			if w.Op == types.OpILikePrefix {
				sb.WriteString(fmt.Sprintf(" ILIKE $%d", argn))
			} else {
				sb.WriteString(fmt.Sprintf(" LIKE $%d", argn))
			}
			args = append(args, fmt.Sprintf("%v%%", w.Value))
			argn++

		default:
			return "", nil, fmt.Errorf("unsupported op %v for column %s", w.Op, col.Name)
		}
	}

	// ORDER BY
	if len(spec.Sort) > 0 {
		sb.WriteString(" ORDER BY ")
		for i, s := range spec.Sort {
			if _, ok := tab.Columns[s.Field]; !ok {
				return "", nil, fmt.Errorf("unknown sort column %q", s.Field)
			}
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(utils.QuoteIdentPG(s.Field))
			if s.Dir == types.Desc {
				sb.WriteString(" DESC")
			} else {
				sb.WriteString(" ASC")
			}
		}
		// tie-break по PK, если задан
		last := spec.Sort[len(spec.Sort)-1].Field
		if tab.PrimaryKey != "" && last != tab.PrimaryKey {
			sb.WriteString(", ")
			sb.WriteString(utils.QuoteIdentPG(tab.PrimaryKey))
			sb.WriteString(" ASC")
		}
	}

	// LIMIT/OFFSET
	if spec.Page != nil {
		limit := spec.Page.Limit
		if limit <= 0 || limit > 1000 {
			limit = 100
		}
		sb.WriteString(fmt.Sprintf(" LIMIT $%d", argn))
		args = append(args, limit)
		argn++

		sb.WriteString(fmt.Sprintf(" OFFSET $%d", argn))
		args = append(args, spec.Page.Offset)
		argn++
	}

	return sb.String(), args, nil
}

func BuildCount(spec types.QuerySpec, sch schemas.Schema) (string, []any, error) {
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
