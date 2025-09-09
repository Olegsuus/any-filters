package types

type ColType int

const (
	ColUnknown ColType = iota
	ColText
	ColNumeric
	ColBool
	ColTime
	ColUUID
	ColJSON
)

type Op int

const (
	OpEq Op = iota
	OpIn
	OpGte
	OpLte
	OpLikePrefix
	OpILikePrefix
)

type SortDir int

const (
	Asc SortDir = iota
	Desc
)

type Condition struct {
	Field string
	Op    Op
	Value any // для OpIn ожидается slice
}

type Sort struct {
	Field string
	Dir   SortDir
}

type Pagination struct {
	Limit  int
	Offset int
}

type QuerySpec struct {
	Table  string
	Select []string
	Where  []Condition
	Sort   []Sort
	Page   *Pagination
}
