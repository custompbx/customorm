package customorm

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

/*

TAG: customsql
PREFIX:
	pkey: - primary key
	fkey: - foreign key
MAIN WORD:
	- column name
ENDING:
	;unique - unique value joining into combined uniq constraint
	;null - without NOT NULL constraint
	;position - field for order position value
	;default= - default value after = sign
	;check() - check constrain
*/

// Constants defining various tags and operands
const (
	primaryKeyTag   = "pkey"
	foreignKeyTag   = "fkey"
	uniqueTag       = "unique"
	indexTag        = "index"
	nullTag         = "null"
	positionTag     = "position"
	defaultTag      = "default"
	checkTag        = "check"
	OperandEqual    = "="
	OperandMore     = ">"
	OperandLess     = "<"
	OperandNotEqual = "<>"
	OperandContains = "CONTAINS"
	OperandIn       = "IN"
)

// Table struct representing a database table
type Table struct {
	Name     string
	Columns  []Column
	FKeys    []FKey
	Uniq     []CompositeFields
	Index    []CompositeFields
	Instance interface{}
}

// Column struct representing a column in a database table
type Column struct {
	Name       string
	FieldName  string
	Value      interface{}
	Type       string
	Attr       string
	IsPosition bool
	Default    string
	Check      string
}

// FKey struct representing a foreign key constraint
type FKey struct {
	ColumnName      string
	ColumnValue     interface{}
	TableName       string
	TableColumnName string
	Type            reflect.Type
	FieldName       string
	IsNull          bool
}

// Filters struct to hold filtering criteria for querying
type Filters struct {
	Fields map[string]FilterFields
	Order  Order
	Limit  int
	Offset int
	Count  bool
	Error  error
}

// CompositeFields struct for field names could be composed wth others
type CompositeFields struct {
	Tag   string
	Group string
}

// FilterFields struct defining individual filtering criteria
type FilterFields struct {
	Flag     bool
	UseValue bool
	Value    interface{}
	Operand  string
}

// Order struct defining ordering criteria
type Order struct {
	Desc   bool     `json:"desc"`
	Fields []string `json:"fields"`
}

// CORM is the main struct for Custom ORM
type CORM struct {
	db *sql.DB
}

// Init initializes the CORM instance with a database connection
func Init(db *sql.DB) *CORM {
	return &CORM{
		db: db,
	}
}

// GetTable retrieves the table structure based on the provided instance
func (c *CORM) GetTable(s interface{}) (Table, error) {
	tableName := GetTableName(s)
	if tableName == "" {
		return Table{}, errors.New("no table name")
	}
	direct := valueIfPtr(s)
	if s == nil {
		return Table{}, errors.New("no table instance")
	}
	table := Table{Name: tableName, Instance: direct}
	table.ImportTableData()
	return table, nil
}

// Row interface to get table name
type Row interface {
	GetTableName() string
}

func (table *Table) ImportTableData() {
	v := reflect.ValueOf(table.Instance)
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag, ok := field.Tag.Lookup("customsql")
		if !ok || tag == "" {
			continue
		}
		isFKey := false
		isPosition := false
		isSerial := false
		ending := " NOT NULL"
		defaultValue := ""
		checkValue := ""
		subConstrain := strings.Split(tag, ";")
		subOption := strings.Split(subConstrain[0], ":")
		tag = subOption[0]
		if len(subOption) > 1 {
			tag = subOption[1]
			switch subOption[0] {
			case foreignKeyTag:
				var columnValue interface{}
				fValue := reflect.Indirect(v.Field(i))
				if fValue.IsValid() {
					columnValue = fValue.FieldByName("Id").Interface()
				}
				table.FKeys = append(table.FKeys, FKey{
					ColumnName:      tag,
					ColumnValue:     columnValue,
					TableName:       GetTableName(reflect.New(v.Field(i).Type().Elem()).Interface()),
					TableColumnName: "id",
					Type:            v.Field(i).Type().Elem(),
					FieldName:       field.Name,
					IsNull:          false,
				},
				)
				isFKey = true
			case primaryKeyTag:
				ending += " PRIMARY KEY"
				isSerial = true
			}
		}
		if len(subConstrain) > 1 {
			for i := 1; i < len(subConstrain); i++ {
				switch true {
				case subConstrain[i] == nullTag:
					ending = ""
				case subConstrain[i] == positionTag:
					isPosition = true
				case len(strings.Split(subConstrain[i], "default=")) > 1:
					if strings.Split(subConstrain[i], "default=")[1] == "" {
						panicErr(errors.New("default arg have wrong format. table:" + table.Name + ". column: " + tag))
					}
					defaultValue = "DEFAULT " + strings.Split(subConstrain[i], "default=")[1]
				case len(strings.Split(subConstrain[i], checkTag)) > 1:
					var checkValue = strings.Split(subConstrain[i], checkTag)[1]
					if checkValue[0] != '(' || checkValue[len(checkValue)-1] != ')' {
						panicErr(errors.New("check arg have wrong format. table:" + table.Name + ". column: " + tag))
					}
					checkValue = "CHECK " + strings.Split(subConstrain[i], checkTag)[1]
				case len(strings.Split(subConstrain[i], indexTag)) > 1:
					table.Index = append(table.Index, CompositeFields{Tag: tag, Group: getIndexAfterUnderline(subConstrain[i], indexTag)})
				case len(strings.Split(subConstrain[i], uniqueTag)) > 1:
					table.Uniq = append(table.Uniq, CompositeFields{Tag: tag, Group: getIndexAfterUnderline(subConstrain[i], uniqueTag)})
				}
			}
		}
		if isFKey {
			continue
		}
		column := Column{
			Name:       tag,
			Value:      v.Field(i).Interface(),
			Attr:       ending,
			FieldName:  field.Name,
			IsPosition: isPosition,
			Default:    defaultValue,
			Check:      checkValue,
		}

		switch field.Type.Name() {
		case "bool":
			column.Type = "BOOLEAN"
		case "int64":
			if isSerial {
				column.Type = "SERIAL"
			} else {
				column.Type = "BIGINT"
			}
		case "int", "uint":
			column.Type = "INTEGER"
		case "string":
			column.Type = "VARCHAR"
		case "float32", "float64":
			column.Type = "FLOAT"
		case "time.Time", "Time":
			column.Type = "TIMESTAMP"
		default:
			log.Printf("Unknown type for column %s: %s", column.Name, field.Type.Name())
			continue
		}
		table.Columns = append(table.Columns, column)
	}
}

// valueIfPtr returns the value if the input is not a pointer, otherwise returns the dereferenced value
func valueIfPtr(s interface{}) interface{} {
	if s == nil {
		return nil
	}
	if reflect.ValueOf(s).Kind() != reflect.Ptr {
		return s
	}
	ptr := reflect.ValueOf(s)
	if ptr.Interface() == nil || !ptr.IsValid() {
		return nil
	}
	value := ptr.Elem()
	if !value.IsValid() {
		return nil
	}
	s = value.Interface()
	//s = reflect.Indirect(ptr)
	return s
}

// Test is a test function for debugging purposes
func (table *Table) Test(s interface{}) {
	v := reflect.ValueOf(s)
	typeOfS := v.Type()

	for i := 0; i < v.NumField(); i++ {
		tag, _ := typeOfS.Field(i).Tag.Lookup("customsql")
		fmt.Printf("Field: %s\tValue: %v\tTag: %v\tType: %v\n", typeOfS.Field(i).Name, v.Field(i).Interface(), tag, typeOfS.Field(i).Type.Name())
	}
}

// ValuesPlaceholders generates placeholders for SQL values
func ValuesPlaceholders(sl []string) string {
	var res string
	for i := 1; i <= len(sl); i++ {
		res += "$" + strconv.Itoa(i)
		if i != len(sl) {
			res += ", "
		}
	}
	return res
}

// ValuesEqualPlaceholders generates equal placeholders for SQL values
func ValuesEqualPlaceholders(sl []string) string {
	var res string
	for i := 1; i <= len(sl); i++ {
		res += sl[i-1] + "=$" + strconv.Itoa(i)
		if i != len(sl) {
			res += ", "
		}
	}
	return res
}

// ValuesEqualPlaceholdersAnd generates equal placeholders separated by "AND" for SQL values
func ValuesEqualPlaceholdersAnd(sl []string) string {
	var res string
	for i := 1; i <= len(sl); i++ {
		res += sl[i-1] + "=$" + strconv.Itoa(i)
		if i != len(sl) {
			res += " AND "
		}
	}
	return res
}

// GetTableName retrieves the table name from the provided instance
func GetTableName(i interface{}) string {
	name := ""
	if row, ok := i.(Row); ok {
		name = row.GetTableName()
	} else {
		if reflect.ValueOf(i).Kind() != reflect.Ptr {
			ptr := reflect.New(reflect.TypeOf(i))
			ptr.Elem().Set(reflect.ValueOf(i))
			if row, ok := ptr.Interface().(Row); ok {
				name = row.GetTableName()
			}
		}
	}
	if name == "" {
		name = getType(i)
	}

	name = ToSnakeCase(name)

	if !isValidTableName(name) {
		name = getType(i)
		name = ToSnakeCase(name)
	}
	return name
}

func isValidTableName(name string) bool {
	tableNameRegex := regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,63}$`)
	return tableNameRegex.MatchString(name)
}

// getType retrieves the type name of the variable
func getType(myvar interface{}) string {
	t := reflect.TypeOf(myvar)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Name()
}

// ToSnakeCase converts a string to snake case
func ToSnakeCase(str string) string {
	matchFirstCap := regexp.MustCompile("([A-Za-z]+)([A-Z][a-z]+)")
	matchAllCap := regexp.MustCompile("([a-z0-9])([A-Z])")

	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

func panicErr(err error) {
	if err != nil {
		panic(err)
	}
}

func (c *Column) toString() string {
	if c.Name == "" || c.Type == "" {
		return ""
	}
	return fmt.Sprintf("%s %s %s %s %s", c.Name, c.Type, c.Attr, c.Default, c.Check)
}

func (f *FKey) toString() string {
	if f.TableColumnName == "" || f.TableName == "" || f.ColumnName == "" {
		return ""
	}
	inNull := "NOT NULL"
	onDelete := "CASCADE"
	if f.IsNull {
		inNull = ""
		onDelete = "SET NULL"
	}
	return fmt.Sprintf("%s bigint %s REFERENCES %s (%s) ON DELETE %s", f.ColumnName, inNull, f.TableName, f.TableColumnName, onDelete)
}

func (table *Table) createTableSql() (string, []string) {
	if len(table.Columns) == 0 {
		panicErr(errors.New("no table struct provided to create table"))
		return "", []string{}
	}
	uniqLines := ""
	if len(table.Uniq) > 0 {
		uniqs := orderedByGroup(table.Uniq)
		for _, u := range uniqs {
			if len(u) == 0 {
				continue
			}
			uniqLines = ",\n UNIQUE (" + strings.Join(u, ", ") + ")"
		}
	}
	var indexLines []string
	if len(table.Index) > 0 {
		inx := orderedByGroup(table.Index)
		for _, i := range inx {
			if len(i) == 0 {
				continue
			}
			indexLines = append(indexLines, "CREATE INDEX IF NOT EXISTS idx_"+table.Name+"_"+strings.Join(i, "_")+" ON "+table.Name+"("+strings.Join(i, ", ")+")")
		}
	}

	var fKeySQL strings.Builder
	for _, val := range table.FKeys {
		fKeySQL.WriteString(val.toString())
		fKeySQL.WriteString(",\n")
	}

	var columnsSQL strings.Builder
	for k, val := range table.Columns {
		columnsSQL.WriteString(val.toString())
		if k != len(table.Columns)-1 {
			columnsSQL.WriteString(",\n")
		}
	}

	sqlReq := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		%s%s%s
	)
	WITH (OIDS=FALSE);`,
		table.Name, fKeySQL.String(), columnsSQL.String(), uniqLines)
	//log.Println(sqlReq)
	return sqlReq, indexLines
}

func isNil(i interface{}) bool {
	if i == nil {
		return true
	}
	switch reflect.TypeOf(i).Kind() {
	case reflect.Ptr, reflect.Map, reflect.Array, reflect.Chan, reflect.Slice:
		return reflect.ValueOf(i).IsNil()
	}
	return false
}
func getIndexAfterUnderline(s, keyWord string) string {
	idx := strings.Index(s, keyWord+"_")
	if idx == -1 {
		return ""
	}

	substr := s[idx+len(keyWord+"_"):]

	// Find the index of the first non-numeric character
	endIdx := 0
	for i, c := range substr {
		if c < '0' || c > '9' {
			endIdx = i
			break
		}
	}
	if endIdx == 0 {
		// If no non-numeric character found, use the entire substring
		endIdx = len(substr)
	}

	return substr[:endIdx]
}
func orderedByGroup(slice []CompositeFields) [][]string {
	// Define a custom sorting function
	sort.Slice(slice, func(i, j int) bool {
		return slice[i].Group < slice[j].Group
	})
	// Extract Field values and join them with commas
	var fields [][]string
	var lastGroup string
	for _, cp := range slice {
		if cp.Group == "" {
			fields = append(fields, []string{cp.Tag})
			continue
		}
		var group = strings.Split(cp.Group, "-")

		if group[0] != lastGroup {
			lastGroup = group[0]
			fields = append(fields, []string{cp.Tag})
			continue
		}
		if len(fields) == 0 {
			fields = append(fields, []string{cp.Tag})
			continue
		}
		fields[len(fields)-1] = append(fields[len(fields)-1], cp.Tag)
	}
	return fields
}

func (f *Filters) ToValue(field string, value interface{}, operand string) *Filters {
	switch operand {
	case OperandEqual:
	case OperandMore:
	case OperandLess:
	case OperandNotEqual:
	case OperandIn:
	case OperandContains:
	default:
		f.Error = errors.New("invalid operand")
		return f
	}
	if f.Fields == nil {
		f.Fields = map[string]FilterFields{}
	}
	useValue := true
	if value == nil {
		useValue = false
	}
	f.Fields[field] = FilterFields{Flag: true, UseValue: useValue, Value: value, Operand: operand}
	return f
}
func (f *Filters) EqualToValue(field string, value interface{}) *Filters {
	if f.Error != nil {
		return f
	}
	return f.ToValue(field, value, OperandEqual)
}

func (f *Filters) MoreToValue(field string, value interface{}) *Filters {
	if f.Error != nil {
		return f
	}
	return f.ToValue(field, value, OperandMore)
}

func (f *Filters) LessToValue(field string, value interface{}) *Filters {
	if f.Error != nil {
		return f
	}
	return f.ToValue(field, value, OperandLess)
}

func (f *Filters) NotEqualToValue(field string, value interface{}) *Filters {
	if f.Error != nil {
		return f
	}
	return f.ToValue(field, value, OperandNotEqual)
}

func (f *Filters) ContainsToValue(field string, value interface{}) *Filters {
	if f.Error != nil {
		return f
	}
	return f.ToValue(field, value, OperandContains)
}
func (f *Filters) InToValue(field string, value interface{}) *Filters {
	if f.Error != nil {
		return f
	}
	return f.ToValue(field, value, OperandIn)
}

func (f *Filters) SetLimit(value int) *Filters {
	if f.Error != nil {
		return f
	}
	if value < 1 {
		f.Error = errors.New("invalid limit")
		return f
	}
	f.Limit = value
	return f
}
func (f *Filters) SetOffset(value int) *Filters {
	if f.Error != nil {
		return f
	}
	if value < 1 {
		f.Error = errors.New("invalid offset")
		return f
	}
	f.Offset = value
	return f
}

func (f *Filters) SetOrder(fieldNames []string, desc bool) *Filters {
	if f.Error != nil {
		return f
	}
	if len(fieldNames) == 0 {
		f.Error = errors.New("no order column names")
		return f
	}
	f.Order = Order{Desc: desc, Fields: fieldNames}
	return f
}
