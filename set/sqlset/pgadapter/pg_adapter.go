/*
Package pgadapter provides an implementation of the
Adapter interface in the sqlset package that works
over a PostgreSQL database.
*/
package pgadapter

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pbanos/botanic/set/sqlset"

	// Import of PostgreSQL driver
	_ "github.com/lib/pq"
)

const (
	discreteValueTableCreateStmt = `CREATE TABLE IF NOT EXISTS discreteValues (
		id SERIAL PRIMARY KEY,
		value TEXT UNIQUE NOT NULL)`

	// MaxDiscreteValueInsertionsPerStatement is the maximum number
	// of discrete values that are allowed to be added with a single
	// insert command with the AddDiscreteValues method of the adapter.
	// Trying to add more will result in making more insertion commands
	MaxDiscreteValueInsertionsPerStatement = 10

	// MaxSampleInsertionsPerStatement is the maximum number
	// of samples that are allowed to be added with a single
	// insert command with the AddSamples method of the adapter.
	// Trying to add more will result in making more insertion commands
	MaxSampleInsertionsPerStatement = 10
)

type adapter struct {
	db *sql.DB
}

/*
New takes a PostgreSQL database connection URL and returns
an Adapter that works on the database or an error if it fails to connect to it.
*/
func New(url string) (sqlset.Adapter, error) {
	db, err := sql.Open("postgres", url)
	if err != nil {
		return nil, err
	}
	return &adapter{db}, nil
}

func (a *adapter) ColumnName(featureName string) (string, error) {
	if featureName == "id" {
		return "", fmt.Errorf(`'%s' is reserved and cannot be used as feature name`, featureName)
	}
	if strings.ContainsAny(featureName, `"`) {
		return "", fmt.Errorf(`feature name '%s' contains invalid character '"'`, featureName)
	}
	return featureName, nil
}

func (a *adapter) CreateDiscreteValuesTable(ctx context.Context) error {
	createStmt, err := a.db.PrepareContext(ctx, discreteValueTableCreateStmt)
	if err != nil {
		return fmt.Errorf("preparing discreteValues creation statement: %v", err)
	}
	defer createStmt.Close()
	_, err = createStmt.ExecContext(ctx)
	if err != nil {
		return fmt.Errorf("running discreteValues creation statement: %v", err)
	}
	return nil
}

func (a *adapter) CreateSampleTable(ctx context.Context, discreteFeatureColumns, continuousFeatureColumns []string) error {
	var createStmtBuf bytes.Buffer
	createStmtBuf.WriteString("CREATE TABLE IF NOT EXISTS samples(")
	for _, c := range discreteFeatureColumns {
		createStmtBuf.WriteString(fmt.Sprintf(`"%s" INTEGER NULL REFERENCES discreteValues(id), `, c))
	}
	for _, c := range continuousFeatureColumns {
		createStmtBuf.WriteString(fmt.Sprintf(`"%s" REAL NULL, `, c))
	}
	createStmtBuf.WriteString(`"id" SERIAL PRIMARY KEY)`)
	createStmt, err := a.db.PrepareContext(ctx, createStmtBuf.String())
	if err != nil {
		return fmt.Errorf("preparing samples creation statement: %v", err)
	}
	defer createStmt.Close()
	_, err = createStmt.ExecContext(ctx)
	if err != nil {
		return fmt.Errorf("ensuring samples table exists: %v", err)
	}
	return nil
}

func (a *adapter) AddDiscreteValues(ctx context.Context, values []string) (int, error) {
	var (
		chunkStart       = 0
		chunkEnd         = MaxDiscreteValueInsertionsPerStatement
		insertStmtBuffer bytes.Buffer
	)
	if len(values) == 0 {
		return 0, nil
	}
	insertStmtStart := "INSERT INTO discreteValues (value) VALUES ($1)"
	if len(values) > MaxDiscreteValueInsertionsPerStatement {
		insertStmtBuffer.WriteString(insertStmtStart)
		for i := 1; i < MaxDiscreteValueInsertionsPerStatement; i++ {
			insertStmtBuffer.WriteString(fmt.Sprintf(", ($%d)", i+1))
		}
		insertStmt, err := a.db.PrepareContext(ctx, insertStmtBuffer.String())
		if err != nil {
			return 0, fmt.Errorf("preparing insert command for %d values: %v", MaxDiscreteValueInsertionsPerStatement, err)
		}
		for c := 0; c < len(values)/MaxDiscreteValueInsertionsPerStatement; c++ {
			iv := make([]interface{}, 0, MaxDiscreteValueInsertionsPerStatement)
			for _, v := range values[chunkStart:chunkEnd] {
				iv = append(iv, v)
			}
			_, err = insertStmt.ExecContext(ctx, iv...)
			if err != nil {
				return chunkStart, fmt.Errorf("inserting the %dth %d values: %v", c+1, MaxDiscreteValueInsertionsPerStatement, err)
			}
			chunkStart += MaxDiscreteValueInsertionsPerStatement
			chunkEnd += MaxDiscreteValueInsertionsPerStatement
		}
		err = insertStmt.Close()
		if err != nil {
			return chunkStart, fmt.Errorf("closing insert command for %d values: %v", MaxDiscreteValueInsertionsPerStatement, err)
		}
	}
	chunkEnd = len(values)
	lastValues := values[chunkStart:chunkEnd]
	if len(lastValues) > 0 {
		insertStmtBuffer = bytes.Buffer{}
		insertStmtBuffer.WriteString(insertStmtStart)
		for i := 1; i < len(lastValues); i++ {
			insertStmtBuffer.WriteString(fmt.Sprintf(", ($%d)", i+1))
		}
		insertStmt, err := a.db.PrepareContext(ctx, insertStmtBuffer.String())
		if err != nil {
			return chunkStart, fmt.Errorf("preparing insert command for %d values: %v", len(lastValues), err)
		}
		ilv := make([]interface{}, 0, len(lastValues))
		for _, v := range lastValues {
			ilv = append(ilv, v)
		}
		_, err = insertStmt.ExecContext(ctx, ilv...)
		if err != nil {
			return chunkStart, fmt.Errorf("inserting the last %d values: %v", len(lastValues), err)
		}
		err = insertStmt.Close()
		if err != nil {
			return chunkEnd, fmt.Errorf("closing insert command for %d values: %v", len(lastValues), err)
		}
	}
	return chunkEnd, nil
}

func (a *adapter) ListDiscreteValues(ctx context.Context) (map[int]string, error) {
	rows, err := a.db.QueryContext(ctx, `SELECT id, value FROM discreteValues`)
	if err != nil {
		return nil, err
	}
	result := make(map[int]string)
	for rows.Next() {
		var id int
		var value string
		err = rows.Scan(&id, &value)
		if err != nil {
			return nil, err
		}
		result[id] = value
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	err = rows.Close()
	return result, err
}

func (a *adapter) AddSamples(ctx context.Context, rawSamples []map[string]interface{}, discreteFeatureColumns, continuousFeatureColumns []string) (int, error) {
	var (
		chunkStart            = 0
		chunkEnd              = MaxSampleInsertionsPerStatement
		insertStmtBuffer      bytes.Buffer
		insertStmtStartBuffer bytes.Buffer
	)
	if len(rawSamples) == 0 {
		return 0, nil
	}
	if len(discreteFeatureColumns)+len(continuousFeatureColumns) == 0 {
		return 0, fmt.Errorf("no features to store")
	}
	insertStmtStartBuffer.WriteString(`INSERT INTO samples ("`)
	insertStmtStartBuffer.WriteString(strings.Join(discreteFeatureColumns, `", "`))
	if len(discreteFeatureColumns) > 0 && len(continuousFeatureColumns) > 0 {
		insertStmtStartBuffer.WriteString(`", "`)
	}
	insertStmtStartBuffer.WriteString(strings.Join(continuousFeatureColumns, `", "`))
	insertStmtStartBuffer.WriteString(`") VALUES ($1`)
	for i := 1; i < len(discreteFeatureColumns)+len(continuousFeatureColumns); i++ {
		insertStmtStartBuffer.WriteString(fmt.Sprintf(", $%d", i+1))
	}
	insertStmtStartBuffer.WriteString(`)`)
	insertStmtStart := insertStmtStartBuffer.String()
	if len(rawSamples) > MaxSampleInsertionsPerStatement {
		insertStmtBuffer.WriteString(insertStmtStart)
		for i := 1; i < MaxSampleInsertionsPerStatement; i++ {
			insertStmtBuffer.WriteString(fmt.Sprintf(", ($%d", 1+i*(len(discreteFeatureColumns)+len(continuousFeatureColumns))))
			for j := 1; j < len(discreteFeatureColumns)+len(continuousFeatureColumns); j++ {
				insertStmtStartBuffer.WriteString(fmt.Sprintf(", $%d", j+1+i*(len(discreteFeatureColumns)+len(continuousFeatureColumns))))
			}
			insertStmtStartBuffer.WriteString(`)`)
		}
		insertStmt, err := a.db.PrepareContext(ctx, insertStmtBuffer.String())
		if err != nil {
			return 0, fmt.Errorf("preparing insert command for %d samples: %v", MaxSampleInsertionsPerStatement, err)
		}
		for c := 0; c < len(rawSamples)/MaxSampleInsertionsPerStatement; c++ {
			irs := make([]interface{}, 0, MaxSampleInsertionsPerStatement*(len(discreteFeatureColumns)+len(continuousFeatureColumns)))
			for _, rs := range rawSamples[chunkStart:chunkEnd] {
				for _, f := range discreteFeatureColumns {
					irs = append(irs, rs[f])
				}
				for _, f := range continuousFeatureColumns {
					irs = append(irs, rs[f])
				}
			}
			_, err = insertStmt.ExecContext(ctx, irs...)
			if err != nil {
				return chunkStart, fmt.Errorf("inserting the %dth %d samples: %v", c+1, MaxSampleInsertionsPerStatement, err)
			}
			chunkStart += MaxSampleInsertionsPerStatement
			chunkEnd += MaxSampleInsertionsPerStatement
		}
		err = insertStmt.Close()
		if err != nil {
			return chunkStart, fmt.Errorf("closing insert command for %d samples: %v", MaxSampleInsertionsPerStatement, err)
		}
	}
	chunkEnd = len(rawSamples)
	lastRawSamples := rawSamples[chunkStart:chunkEnd]
	if len(lastRawSamples) > 0 {
		insertStmtBuffer = bytes.Buffer{}
		insertStmtBuffer.WriteString(insertStmtStart)
		for i := 1; i < len(lastRawSamples); i++ {
			insertStmtBuffer.WriteString(fmt.Sprintf(", ($%d", 1+i*(len(discreteFeatureColumns)+len(continuousFeatureColumns))))
			for j := 1; j < len(discreteFeatureColumns)+len(continuousFeatureColumns); j++ {
				insertStmtStartBuffer.WriteString(fmt.Sprintf(", $%d", j+1+i*(len(discreteFeatureColumns)+len(continuousFeatureColumns))))
			}
			insertStmtStartBuffer.WriteString(`)`)
		}
		insertStmt, err := a.db.PrepareContext(ctx, insertStmtBuffer.String())
		if err != nil {
			return chunkStart, fmt.Errorf("preparing insert command for %d values: %v", len(lastRawSamples), err)
		}
		ilrs := make([]interface{}, 0, len(lastRawSamples)*(len(discreteFeatureColumns)+len(continuousFeatureColumns)))
		for _, rs := range rawSamples[chunkStart:chunkEnd] {
			for _, f := range discreteFeatureColumns {
				ilrs = append(ilrs, rs[f])
			}
			for _, f := range continuousFeatureColumns {
				ilrs = append(ilrs, rs[f])
			}
		}
		_, err = insertStmt.ExecContext(ctx, ilrs...)
		if err != nil {
			return chunkStart, fmt.Errorf("inserting the last %d values: %v", len(lastRawSamples), err)
		}
		err = insertStmt.Close()
		if err != nil {
			return chunkEnd, fmt.Errorf("closing insert command for %d values: %v", len(lastRawSamples), err)
		}
	}
	return chunkEnd, nil
}

func (a *adapter) ListSamples(ctx context.Context, criteria []*sqlset.FeatureCriterion, discreteFeatureColumns, continuousFeatureColumns []string) ([]map[string]interface{}, error) {
	var result []map[string]interface{}
	err := a.IterateOnSamples(
		ctx,
		criteria,
		discreteFeatureColumns,
		continuousFeatureColumns,
		func(_ int, rawSample map[string]interface{}) (bool, error) {
			result = append(result, rawSample)
			return true, nil
		})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (a *adapter) IterateOnSamples(ctx context.Context, criteria []*sqlset.FeatureCriterion, discreteFeatureColumns, continuousFeatureColumns []string, lambda func(int, map[string]interface{}) (bool, error)) error {
	var queryBuffer bytes.Buffer
	var whereValues []interface{}
	queryBuffer.WriteString(`SELECT "`)
	queryBuffer.WriteString(strings.Join(discreteFeatureColumns, `", "`))
	if len(discreteFeatureColumns) > 0 && len(continuousFeatureColumns) > 0 {
		queryBuffer.WriteString(`", "`)
	}
	queryBuffer.WriteString(strings.Join(continuousFeatureColumns, `", "`))
	queryBuffer.WriteString(`" FROM samples`)
	if len(criteria) > 0 {
		var whereClause string
		whereClause, whereValues = buildWhereClause(criteria)
		queryBuffer.WriteString(whereClause)
	}
	rows, err := a.db.QueryContext(ctx, queryBuffer.String(), whereValues...)
	if err != nil {
		return err
	}
	for j := 0; rows.Next(); j++ {
		rawSample := make(map[string]interface{})
		discreteValues := make([]sql.NullInt64, len(discreteFeatureColumns))
		continuousValues := make([]sql.NullFloat64, len(continuousFeatureColumns))
		values := make([]interface{}, 0, len(discreteFeatureColumns)+len(continuousFeatureColumns))
		for i := range discreteValues {
			values = append(values, &discreteValues[i])
		}
		for i := range continuousValues {
			values = append(values, &continuousValues[i])
		}
		err = rows.Scan(values...)
		if err != nil {
			return err
		}
		for i, c := range discreteFeatureColumns {
			if discreteValues[i].Valid {
				rawSample[c] = int(discreteValues[i].Int64)
			}
		}
		for i, c := range continuousFeatureColumns {
			if continuousValues[i].Valid {
				rawSample[c] = continuousValues[i].Float64
			}
		}
		ok, err := lambda(j, rawSample)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
	}
	err = rows.Err()
	if err != nil {
		return err
	}
	err = rows.Close()
	return err
}

func (a *adapter) CountSamples(ctx context.Context, criteria []*sqlset.FeatureCriterion) (int, error) {
	var queryBuffer bytes.Buffer
	var whereValues []interface{}
	queryBuffer.WriteString(`SELECT COUNT(*) FROM samples`)
	if len(criteria) > 0 {
		var whereClause string
		whereClause, whereValues = buildWhereClause(criteria)
		queryBuffer.WriteString(whereClause)
	}
	rows, err := a.db.QueryContext(ctx, queryBuffer.String(), whereValues...)
	if err != nil {
		return 0, err
	}
	if !rows.Next() {
		return 0, rows.Err()
	}
	var count int
	err = rows.Scan(&count)
	if err != nil {
		return 0, err
	}
	err = rows.Close()
	return count, err
}

func (a *adapter) ListSampleDiscreteFeatureValues(ctx context.Context, fc string, criteria []*sqlset.FeatureCriterion) ([]int, error) {
	var queryBuffer bytes.Buffer
	var whereValues []interface{}
	queryBuffer.WriteString(fmt.Sprintf(`SELECT DISTINCT "%s" FROM samples`, fc))
	if len(criteria) > 0 {
		var whereClause string
		whereClause, whereValues = buildWhereClause(criteria)
		queryBuffer.WriteString(whereClause)
	}
	rows, err := a.db.QueryContext(ctx, queryBuffer.String(), whereValues...)
	if err != nil {
		return nil, err
	}
	var result []int
	for rows.Next() {
		var value sql.NullInt64
		err = rows.Scan(&value)
		if err != nil {
			return nil, err
		}
		if value.Valid {
			result = append(result, int(value.Int64))
		}
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	err = rows.Close()
	return result, err
}

func (a *adapter) ListSampleContinuousFeatureValues(ctx context.Context, fc string, criteria []*sqlset.FeatureCriterion) ([]float64, error) {
	var queryBuffer bytes.Buffer
	var whereValues []interface{}
	queryBuffer.WriteString(fmt.Sprintf(`SELECT DISTINCT "%s" FROM samples`, fc))
	if len(criteria) > 0 {
		var whereClause string
		whereClause, whereValues = buildWhereClause(criteria)
		queryBuffer.WriteString(whereClause)
	}
	rows, err := a.db.QueryContext(ctx, queryBuffer.String(), whereValues...)
	if err != nil {
		return nil, err
	}
	var result []float64
	for rows.Next() {
		var value sql.NullFloat64
		err = rows.Scan(&value)
		if err != nil {
			return nil, err
		}
		if value.Valid {
			result = append(result, value.Float64)
		}
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	err = rows.Close()
	return result, err
}

func (a *adapter) CountSampleDiscreteFeatureValues(ctx context.Context, fc string, criteria []*sqlset.FeatureCriterion) (map[int]int, error) {
	var queryBuffer bytes.Buffer
	var whereValues []interface{}
	queryBuffer.WriteString(fmt.Sprintf(`SELECT "%s", COUNT("%s") FROM samples`, fc, fc))
	if len(criteria) > 0 {
		var whereClause string
		whereClause, whereValues = buildWhereClause(criteria)
		queryBuffer.WriteString(whereClause)
	}
	queryBuffer.WriteString(fmt.Sprintf(` GROUP BY "%s"`, fc))
	rows, err := a.db.QueryContext(ctx, queryBuffer.String(), whereValues...)
	if err != nil {
		return nil, err
	}
	result := make(map[int]int)
	for rows.Next() {
		var value sql.NullInt64
		var count int
		err = rows.Scan(&value, &count)
		if err != nil {
			return nil, err
		}
		if value.Valid {
			result[int(value.Int64)] = count
		}
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	err = rows.Close()
	return result, err
}

func (a *adapter) CountSampleContinuousFeatureValues(ctx context.Context, fc string, criteria []*sqlset.FeatureCriterion) (map[float64]int, error) {
	var queryBuffer bytes.Buffer
	var whereValues []interface{}
	queryBuffer.WriteString(fmt.Sprintf(`SELECT "%s", COUNT("%s") FROM samples`, fc, fc))
	if len(criteria) > 0 {
		var whereClause string
		whereClause, whereValues = buildWhereClause(criteria)
		queryBuffer.WriteString(whereClause)
	}
	queryBuffer.WriteString(fmt.Sprintf(` GROUP BY "%s"`, fc))
	rows, err := a.db.QueryContext(ctx, queryBuffer.String(), whereValues...)
	if err != nil {
		return nil, err
	}
	result := make(map[float64]int)
	for rows.Next() {
		var value sql.NullFloat64
		var count int
		err = rows.Scan(&value, &count)
		if err != nil {
			return nil, err
		}
		if value.Valid {
			result[value.Float64] = count
		}
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	err = rows.Close()
	return result, err
}

func buildWhereClause(criteria []*sqlset.FeatureCriterion) (string, []interface{}) {
	if len(criteria) == 0 {
		return "", nil
	}
	var buf bytes.Buffer
	values := make([]interface{}, 0, len(criteria))
	buf.WriteString(" WHERE ")
	buf.WriteString(fmt.Sprintf(`"%s" %s $1`, criteria[0].FeatureColumn, criteria[0].Operator))
	values = append(values, criteria[0].Value)
	for i := 1; i < len(criteria); i++ {
		buf.WriteString(fmt.Sprintf(`AND "%s" %s $%d`, criteria[i].FeatureColumn, criteria[i].Operator, i+1))
		values = append(values, criteria[i].Value)
	}
	return buf.String(), values
}
