package utils

import "strings"

// QuoteIdentPG — безопасный квотинг идентификатора для PostgreSQL: "na""me"
func QuoteIdentPG(s string) string {
	if s == "" {
		return `""`
	}
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
