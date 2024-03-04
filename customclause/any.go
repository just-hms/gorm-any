package customclause

import (
	"database/sql"
	"log"
	"slices"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	whereClause     = clause.Where{}.Name()
	postgresDialect = postgres.Dialector{}.Name()
)

// ANY is a custom implementation of the clause.IN which binds array of Values directly to a single variable
// it has been implemented to address the limitation of "protocol limited to 65535 parameters".
type ANY struct {
	clause.IN
}

// UseAny configures the DB to use the ANY type for IN clauses to resolve parameter limitations.
func UseAny(db *gorm.DB) {
	currentDialect := db.Dialector.Name()
	if currentDialect != postgresDialect {
		log.Printf("ANY clause not supported with %q dialect", currentDialect)
		return
	}

	db.ClauseBuilders[whereClause] = func(c clause.Clause, builder clause.Builder) {
		where := c.Expression.(clause.Where)
		for i, expr := range where.Exprs {
			if in, ok := expr.(clause.IN); ok {
				where.Exprs[i] = ANY{IN: in}
			}
		}
		c.Build(builder)
	}
}

// Build constructs the postgres ANY clause, used to make queries with large value lists work
func (c ANY) Build(builder clause.Builder) {
	// Only replace clause.IN with ANY for value lists, not subqueries
	hasNonValue := slices.ContainsFunc(c.Values, func(v any) bool {
		switch v.(type) {
		case sql.NamedArg, clause.Column,
			clause.Table, clause.Interface, clause.Expression,
			[]any, *gorm.DB:
			return true
		}
		return false
	})

	// use clause.IN as default
	if hasNonValue || len(c.Values) <= 1 {
		c.IN.Build(builder)
		return
	}

	builder.WriteQuoted(c.Column)
	stmt := builder.(*gorm.Statement)

	// actual binding of the array
	// replacing `IN ($1, $2, $3)` with `= ANY ($1)`
	// which then translates to `= ANY([element, element2, element3, ...])`
	_, _ = builder.WriteString(" = ANY (")
	addBulk(stmt, c.Values)
	_, _ = builder.WriteString(")")
}

// addBulk integrates a list of values into the query, leveraging postgres's array binding support
func addBulk(stmt *gorm.Statement, v any) {
	stmt.Vars = append(stmt.Vars, v)
	stmt.DB.Dialector.BindVarTo(stmt, stmt, v)
}
