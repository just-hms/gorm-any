package customclause

import (
	"database/sql/driver"
	"fmt"
	"log"
	"slices"
	"strings"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	postgresDialect = postgres.Dialector{}.Name()
	whereClause     = clause.Where{}.Name()
)

// ANY reimplements clause.IN from gorm defaults; used to mitigate the "protocol limited to 65535 parameters" limitation
type ANY struct {
	clause.IN
}

// UseAny makes ANY the default behaviour in IN clauses of driver.Valuer types
func UseAny(db *gorm.DB) {
	currentDialect := db.Dialector.Name()
	if currentDialect != postgresDialect {
		log.Printf(
			"current dialect is %q, ANY(ARRAY[]) syntax can be used only with %q, will use default IN behaviour",
			currentDialect,
			postgresDialect,
		)
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

// Build a custom postgres ANY clause and write it to the [gorm.Statement] wire
func (c ANY) Build(builder clause.Builder) {
	// check that all the c.Values are values and not clause.Expression
	// - this cluase can replace the clause.IN only if the query that replaces is :
	// ... WHERE id IN ("1", "2")
	// and not
	// ... WHERE id IN (SELECT ...)
	areAllValues := slices.ContainsFunc(c.Values, func(a any) bool {
		_, ok := a.(driver.Valuer)
		return !ok
	})
	// retrun the default behaviour also with 0 or 1 Values
	if !areAllValues || len(c.Values) <= 1 {
		c.IN.Build(builder)
		return
	}
	builder.WriteQuoted(c.Column)

	// extract the database type of the column values
	stmt := builder.(*gorm.Statement)
	dataType := getSchemaFieldType(c.Column.(clause.Column), stmt)

	// write the ANY array to the wire
	_, _ = builder.WriteString(" = ANY (ARRAY[")
	builder.AddVar(builder, c.Values...)
	_, _ = builder.WriteString(fmt.Sprintf("]::%s[])", dataType)) // ANY (ARRAY['value1', 'value2', 'value3']::uuid[]);
}

// getSchemaFieldType return the database driver column type extrascted from the provided [clause.Column]
func getSchemaFieldType(column clause.Column, stmt *gorm.Statement) string {

	// get the field name by passing only the column name to stmt.Quote
	// this:
	// - solves column name being : ~~py~~
	// - retrieves the name without the table being included // stmt.Quote(column) = "tableName"."ColumnName"
	fieldName := stmt.Quote(clause.Column{Name: column.Name})
	fieldName = strings.Trim(fieldName, `"`)

	// get the schema field from the name
	field := stmt.Schema.LookUpField(fieldName)
	if field == nil {
		_ = stmt.DB.AddError(gorm.ErrModelAccessibleFieldsRequired)
		return ""
	}

	// get the actual postgres representation of the schema type
	return stmt.Dialector.DataTypeOf(field)
}
