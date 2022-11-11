package customorm

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/lib/pq"
	"log"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

/*

TAG: customorm
PREFIX:
	pkey: - primary key
	fkey: - foreign key
MAIN WORD:
	- column name
ENDING:
	;uniq - unique value joining into combined uniq constraint
	;null - without NOT NULL constraint
	;position - field for order position value
	;default= -default value after = sign
*/

type Filters struct {
	Fields map[string]FilterFields
	Order  Order
	Limit  int
	Offset int
	Count  bool
}

type FilterFields struct {
	Flag     bool
	UseValue bool
	Value    interface{}
	Operand  string
}

type Order struct {
	Desc   bool     `json:"desc"`
	Fields []string `json:"fields"`
}

func Init(db *sql.DB) *CORM {
	return &CORM{
		db: db,
	}
}

func (c *CORM) CreateTable(s interface{}) bool {
	table, err := c.GetTable(s)
	if err != nil {
		panicErr(err)
		return false
	}

	sqlReq := table.createTableSql()
	if sqlReq == "" {
		panicErr(errors.New("cant create table " + table.Name))
	}
	_, err = c.db.Exec(sqlReq)

	panicErr(err)
	if err != nil {
		log.Printf("%+v", err)
		return false
	}

	return true
}

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

func (c *CORM) InsertRow(s interface{}) (int64, error) {
	var id int64
	table, err := c.GetTable(s)
	if err != nil {
		return 0, err
	}
	var names []string
	var values []interface{}
	var parentColumnName string
	var parentIndex int
	for _, v := range table.FKeys {
		if !v.IsNull && (v.ColumnValue == nil || v.ColumnValue == 0 || v.ColumnValue == "" || v.ColumnValue == false) {
			return 0, errors.New("empty foreign key value")
		}

		if !v.IsNull {
			parentColumnName = v.ColumnName
			parentIndex = len(names) + 1
		}

		names = append(names, v.ColumnName)
		values = append(values, v.ColumnValue)
	}
	var positionSql string
	var positionColumnName string
	for _, v := range table.Columns {
		if v.Name == "id" {
			continue
		}
		if v.IsPosition {
			positionColumnName = v.Name
			positionSql = fmt.Sprintf("(SELECT COALESCE((SELECT %s + 1 FROM %s WHERE %s = $%d ORDER BY %s DESC LIMIT 1), 1))", v.Name, table.Name, parentColumnName, parentIndex, v.Name)
			continue
		}
		names = append(names, v.Name)
		values = append(values, v.Value)
	}
	placeholders := ValuesPlaceholders(names)
	if len(names) != 0 && positionSql != "" {
		positionSql = ", " + positionSql
		names = append(names, positionColumnName)
	}
	sqlReq := fmt.Sprintf("INSERT INTO %s(%s) VALUES(%s%s) returning id;", table.Name, strings.Join(names, ", "), placeholders, positionSql)

	err = c.db.QueryRow(sqlReq, values...).Scan(&id)
	if err != nil {
		return 0, err
	}
	if id == 0 {
		return 0, errors.New("no new id returned")
	}

	return id, nil
}

func (c *CORM) DeleteRowById(s interface{}) error {
	tableName := GetTableName(s)
	if tableName == "" {
		return errors.New("no table name")
	}
	direct := valueIfPtr(s)
	if s == nil {
		return errors.New("no table instance")
	}
	r := reflect.ValueOf(direct)
	f := reflect.Indirect(r).FieldByName("Id")
	if !f.IsValid() || f.Int() == 0 {
		return errors.New("no id value")
	}

	sqlReq := fmt.Sprintf("DELETE FROM %s WHERE id = $1;", tableName)
	_, err := c.db.Exec(sqlReq, f.Int())
	if err != nil {
		return err
	}

	return nil
}

func (c *CORM) DeleteRowByArgId(s interface{}, id int64) error {
	if id == 0 {
		return errors.New("no id value")
	}
	tableName := GetTableName(s)
	if tableName == "" {
		return errors.New("no table name")
	}
	sqlReq := fmt.Sprintf("DELETE FROM %s WHERE id = $1;", tableName)
	_, err := c.db.Exec(sqlReq, id)
	if err != nil {
		return err
	}

	return nil
}

func (c *CORM) DeleteRows(s interface{}, fieldNames map[string]bool) error {
	table, err := c.GetTable(s)
	if err != nil {
		return err
	}
	var names []string
	var values []interface{}
	var updatePos bool
	for _, v := range table.Columns {
		if !fieldNames[v.FieldName] {
			continue
		}
		names = append(names, v.Name)
		values = append(values, v.Value)
	}
	for _, v := range table.FKeys {
		if !fieldNames[v.FieldName] {
			continue
		}
		names = append(names, v.ColumnName)
		values = append(values, v.ColumnValue)
	}
	if len(names) == 0 {
		if updatePos {
			return nil
		}
		return errors.New("no fields to delete")
	}
	sqlReq := fmt.Sprintf("DELETE FROM %s WHERE %s;", table.Name, ValuesEqualPlaceholdersAnd(names))
	_, err = c.db.Exec(sqlReq, values...)
	if err != nil {
		return err
	}

	return nil
}

func (c *CORM) UpdateRow(s interface{}, onlyFields bool, fieldNames map[string]bool) error {
	table, err := c.GetTable(s)
	if err != nil {
		return err
	}
	var names []string
	var values []interface{}
	var itemId int64
	var updatePos bool
	for _, v := range table.Columns {
		if v.Name == "id" {
			itemId = v.Value.(int64)
			continue
		}
		if onlyFields && !fieldNames[v.FieldName] {
			continue
		}
		if v.IsPosition {
			err = c.MovePosition(table)
			if err != nil {
				return err
			}
			if len(fieldNames) == 1 {
				return nil
			}
			continue
		}
		names = append(names, v.Name)
		values = append(values, v.Value)
	}

	if itemId == 0 {
		return errors.New("no row id")
	}

	for _, v := range table.FKeys {
		if onlyFields && !fieldNames[v.FieldName] {
			continue
		}
		names = append(names, v.ColumnName)
		values = append(values, v.ColumnValue)
	}
	if len(names) == 0 {
		if updatePos {
			return nil
		}
		return errors.New("no fields to update")
	}
	sqlReq := fmt.Sprintf("UPDATE %s SET %s WHERE id = %d;", table.Name, ValuesEqualPlaceholders(names), itemId)
	_, err = c.db.Exec(sqlReq, values...)
	if err != nil {
		return err
	}

	return nil
}

// rarely used
func (c *CORM) GetDataAll(s interface{}, asMap bool) (interface{}, error) {
	table, err := c.GetTable(s)
	if err != nil {
		return nil, err
	}
	var maxLimit = 100000
	var names []string
	indirect := reflect.ValueOf(table.Instance)

	for _, v := range table.Columns {
		names = append(names, v.Name)
	}
	for i := 0; i < indirect.NumField(); i++ {
		if indirect.Field(i).Kind() != reflect.Ptr {
			continue
		}
		for _, v := range table.FKeys {
			if indirect.Type().Field(i).Type.Elem() != v.Type {
				continue
			}
			name := v.ColumnName
			if v.IsNull {
				//TODO: for now int only
				name = fmt.Sprintf("COALESCE(%s, 0)", v.ColumnName)
			}
			names = append(names, name)
		}
	}

	sqlReq := fmt.Sprintf(`SELECT %s FROM %s LIMIT %d;`, strings.Join(names, ", "), table.Name, maxLimit)
	results, err := c.db.Query(sqlReq)
	if err != nil {
		log.Printf("%+v", err)
		return nil, err
	}
	defer results.Close()
	var res []interface{}
	var resMap = make(map[int64]interface{})

	for results.Next() {
		var ptrs []interface{}
		var idPtr reflect.Value
		newIndirect := reflect.New(indirect.Type()).Elem()
		for i := 0; i < newIndirect.NumField(); i++ {
			f := newIndirect.Field(i)
			t := newIndirect.Type().Field(i)
			if asMap && t.Name == "Id" && t.Type.Kind() == reflect.Int64 {
				idPtr = f.Addr()
			}
			tag, ok := t.Tag.Lookup("customsql")
			if !ok || tag == "" {
				continue
			}
			if f.Kind() == reflect.Ptr {
				for _, v := range table.FKeys {
					if newIndirect.Type().Field(i).Type.Elem() != v.Type {
						continue
					}
					newValPkey := reflect.New(v.Type)
					f2 := newValPkey.Elem().FieldByName("Id")
					ptrs = append(ptrs, f2.Addr().Interface())
					f.Set(newValPkey)
				}
				continue
			}
			ptrs = append(ptrs, f.Addr().Interface())
		}

		err = results.Scan(ptrs...)
		if err != nil {
			log.Printf("%+v", err)
			return nil, err
		}
		if asMap {
			resMap[idPtr.Elem().Int()] = newIndirect.Interface()
		} else {
			res = append(res, newIndirect.Interface())
		}
	}
	if asMap {
		delete(resMap, 0)
		return resMap, nil
	}
	return res, nil
}

func (c *CORM) GetDataById(s interface{}, id int64) (interface{}, error) {
	table, err := c.GetTable(s)
	if err != nil {
		return nil, err
	}
	var names []string
	var itemId interface{}
	var ptrs []interface{}

	for _, v := range table.Columns {
		if v.Name == "id" {
			itemId = v.Value
		}
		names = append(names, v.Name)
	}
	if id != 0 {
		itemId = id
	}
	indirect := reflect.ValueOf(table.Instance)
	newIndirect := reflect.New(indirect.Type()).Elem()

	for i := 0; i < newIndirect.NumField(); i++ {
		f := newIndirect.Field(i)
		t := newIndirect.Type().Field(i)
		tag, ok := t.Tag.Lookup("customsql")
		if !ok || tag == "" {
			continue
		}
		if f.Kind() == reflect.Ptr {
			for _, v := range table.FKeys {
				if newIndirect.Type().Field(i).Type.Elem() != v.Type {
					continue
				}
				newValPkey := reflect.New(v.Type)
				f2 := newValPkey.Elem().FieldByName("Id")
				ptrs = append(ptrs, f2.Addr().Interface())
				name := v.ColumnName
				if v.IsNull {
					//TODO: for now int only
					name = fmt.Sprintf("COALESCE(%s, 0)", v.ColumnName)
				}
				names = append(names, name)
				f.Set(newValPkey)
			}
			continue
		}
		ptrs = append(ptrs, f.Addr().Interface())
	}

	sqlReq := fmt.Sprintf(`SELECT %s FROM %s WHERE id = %d;`, strings.Join(names, ", "), table.Name, itemId)
	row := c.db.QueryRow(sqlReq)

	err = row.Scan(ptrs...)
	if err != nil {
		log.Printf("%+v", err)
		return nil, err
	}

	return newIndirect.Interface(), nil
}

func (c *CORM) GetDataByValue(s interface{}, filter Filters, asMap bool) (interface{}, error) {
	if len(filter.Fields) == 0 {
		return nil, errors.New("no values")
	}
	table, err := c.GetTable(s)
	if err != nil {
		return nil, err
	}
	var maxLimit = 100000
	var names []string
	var wheres []string
	var wheresArgs []interface{}
	var offset string
	var order string
	var positionColumnName string
	var primaryKeyColumnName string
	indirect := reflect.ValueOf(table.Instance)

	p := 1
	for _, v := range table.Columns {
		if v.IsPosition {
			positionColumnName = v.Name
		}
		names = append(names, v.Name)
		var fName string

		if filter.Fields[v.FieldName].Flag {
			fName = v.FieldName
		} else if filter.Fields[v.Name].Flag {
			fName = v.Name
		}

		if fName == "" {
			continue
		}
		operand := "="
		postOperand := ""
		val := v.Value
		if filter.Fields[fName].UseValue {
			val = filter.Fields[fName].Value
		}

		switch filter.Fields[fName].Operand {
		case ">":
			operand = filter.Fields[fName].Operand
		case "<":
			operand = filter.Fields[fName].Operand
		case "CONTAINS":
			operand = "LIKE '%' || "
			postOperand = " || '%'"
		case "IN":
			operand = "= ANY("
			postOperand = ")"
			switch val.(type) {
			case []int64:
				value := val.([]int64)
				val = pq.Array(value)
			case []string:
				value := val.([]string)
				val = pq.Array(value)
			default:
				return nil, errors.New("wrong IN value")
			}
		}
		wheres = append(wheres, v.Name+" "+operand+" $"+strconv.Itoa(p)+postOperand)

		wheresArgs = append(wheresArgs, val)
		p++
	}

	for i := 0; i < indirect.NumField(); i++ {
		if indirect.Field(i).Kind() != reflect.Ptr {
			continue
		}
		for _, v := range table.FKeys {
			if indirect.Type().Field(i).Type.Elem() != v.Type {
				continue
			}
			name := v.ColumnName
			if v.IsNull {
				//TODO: for now int only
				name = fmt.Sprintf("COALESCE(%s, 0)", v.ColumnName)
			}
			names = append(names, name)
			//TODO: for one key only for now
			primaryKeyColumnName = name
			/*			if isNil(v.ColumnValue) || v.ColumnValue == "" || v.ColumnValue == 0 || v.ColumnValue == false {
						continue
					}*/
			if !filter.Fields[v.FieldName].Flag {
				continue
			}
			operand := "="
			postOperand := ""
			val := v.ColumnValue
			if filter.Fields[v.FieldName].UseValue {
				val = filter.Fields[v.FieldName].Value
			}
			switch filter.Fields[v.FieldName].Operand {
			case "IN":
				operand = "= ANY("
				postOperand = ")"
				switch val.(type) {
				case []int64:
					value := val.([]int64)
					val = pq.Array(value)
				default:
					return nil, errors.New("wrong IN value")
				}
			}
			wheres = append(wheres, v.ColumnName+" "+operand+" $"+strconv.Itoa(p)+postOperand)
			wheresArgs = append(wheresArgs, val)
			p++
		}
	}

	if len(wheres) == 0 && filter.Limit == 0 {
		return nil, errors.New("no search values")
	}

	if len(filter.Order.Fields) > 0 {
		desc := "ASC"
		if filter.Order.Desc {
			desc = "DESC"
		}

		order = fmt.Sprintf("ORDER BY %s %s ", strings.Join(filter.Order.Fields, ", "), desc)
	} else if !asMap && (positionColumnName != "" || primaryKeyColumnName != "") {
		var args []string
		if primaryKeyColumnName != "" {
			args = append(args, primaryKeyColumnName)
		}
		if positionColumnName != "" {
			args = append(args, positionColumnName)
		}

		//TODO: now its just automatic way
		order = fmt.Sprintf("ORDER BY %s ", strings.Join(args, ", "))
	}

	if filter.Limit != 0 && filter.Limit < maxLimit {
		maxLimit = filter.Limit
	}
	if filter.Offset != 0 {
		offset = fmt.Sprintf("OFFSET %d", filter.Offset)
	}
	where := "WHERE "
	if len(wheres) == 0 {
		where = ""
	}
	sqlReq := fmt.Sprintf(
		`SELECT %s FROM %s %s %s LIMIT %d %s;`,
		strings.Join(names, ", "),
		table.Name,
		where+strings.Join(wheres, " AND "),
		order,
		maxLimit,
		offset,
	)
	if filter.Count {
		sqlReq = fmt.Sprintf(
			`SELECT %s FROM %s %s;`,
			"COUNT(*)",
			table.Name,
			where+strings.Join(wheres, " AND "),
		)
		var count int64
		err = c.db.QueryRow(sqlReq, wheresArgs...).Scan(&count)
		if err != nil {
			log.Println(sqlReq)
			log.Println(wheresArgs)
			log.Printf("%+v", err)
			return nil, err
		}
		return []interface{}{count}, nil
	}
	//log.Println(sqlReq)
	//log.Println(wheresArgs)
	results, err := c.db.Query(sqlReq, wheresArgs...)
	if err != nil {
		log.Println(sqlReq)
		log.Println(wheresArgs)
		log.Printf("%+v", err)
		return nil, err
	}
	defer results.Close()
	var res []interface{}
	var resMap = make(map[int64]interface{})
	for results.Next() {
		var ptrs []interface{}
		var idPtr reflect.Value
		newIndirect := reflect.New(indirect.Type()).Elem()
		for i := 0; i < newIndirect.NumField(); i++ {
			f := newIndirect.Field(i)
			t := newIndirect.Type().Field(i)
			if asMap && t.Name == "Id" && t.Type.Kind() == reflect.Int64 {
				idPtr = f.Addr()
			}
			tag, ok := t.Tag.Lookup("customsql")
			if !ok || tag == "" {
				continue
			}

			if f.Kind() == reflect.Ptr {
				for _, v := range table.FKeys {
					if newIndirect.Type().Field(i).Type.Elem() != v.Type {
						continue
					}
					newValPkey := reflect.New(v.Type)
					f2 := newValPkey.Elem().FieldByName("Id")
					ptrs = append(ptrs, f2.Addr().Interface())
					f.Set(newValPkey)

				}
				continue
			}
			ptrs = append(ptrs, f.Addr().Interface())
		}

		err = results.Scan(ptrs...)
		if err != nil {
			log.Printf("%+v", err)
			return nil, err
		}
		if asMap {
			resMap[idPtr.Elem().Int()] = newIndirect.Interface()
		} else {
			res = append(res, newIndirect.Interface())
		}
	}
	if asMap {
		delete(resMap, 0)
		return resMap, nil
	}
	return res, nil
}

type Row interface {
	GetTableName() string
}

type CORM struct {
	db *sql.DB
}

func (table *Table) ImportTableData() {
	v := reflect.ValueOf(table.Instance)
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		notNeed := false
		isUniq := false
		isNull := false
		isPosition := false
		isSerial := false
		ending := " NOT NULL"
		defaultValue := ""
		checkValue := ""
		tag, ok := t.Field(i).Tag.Lookup("customsql")
		if !ok || tag == "" {
			continue
		}
		subConstrain := strings.Split(tag, ";")
		if len(subConstrain) > 1 {
			tag = subConstrain[0]
			for i := 1; i < len(subConstrain); i++ {
				switch true {
				case subConstrain[i] == "unique":
					isUniq = true
				case subConstrain[i] == "null":
					isNull = true
					ending = ""
				case subConstrain[i] == "position":
					isPosition = true
				case len(strings.Split(subConstrain[i], "default=")) > 1:
					defaultValue = "DEFAULT " + strings.Split(subConstrain[i], "default=")[1]
				case len(strings.Split(subConstrain[i], "check")) > 1:
					checkValue = "CHECK " + strings.Split(subConstrain[i], "check")[1]
				}
			}
		}
		subOption := strings.Split(tag, ":")
		if len(subOption) > 1 {
			tag = subOption[1]
			switch subOption[0] {
			case "fkey":
				var columnValue interface{}
				fValue := reflect.Indirect(v.Field(i))
				if fValue.IsValid() {
					columnValue = fValue.FieldByName("Id").Interface()
				}
				table.FKeys = append(table.FKeys, FKey{
					ColumnName:      subOption[1],
					ColumnValue:     columnValue,
					TableName:       GetTableName(reflect.New(v.Field(i).Type().Elem()).Interface()),
					TableColumnName: "id",
					Type:            v.Field(i).Type().Elem(),
					FieldName:       t.Field(i).Name,
					IsNull:          isNull,
				},
				)
				notNeed = true
			case "pkey":
				ending += " PRIMARY KEY"
				isSerial = true
			}
		}
		if isUniq {
			table.Uniq = append(table.Uniq, tag)
		}
		if notNeed {
			continue
		}
		column := Column{
			Name:       tag,
			Value:      v.Field(i).Interface(),
			Attr:       ending,
			FieldName:  t.Field(i).Name,
			IsPosition: isPosition,
			Default:    defaultValue,
			Check:      checkValue,
		}
		switch t.Field(i).Type.Name() {
		case "bool":
			column.Type = "BOOLEAN"
			table.Columns = append(table.Columns, column)
		case "int64":
			if isSerial {
				column.Type = "SERIAL"
			} else {
				column.Type = "BIGINT"
			}
			table.Columns = append(table.Columns, column)
		case "int", "uint":
			column.Type = "INTEGER"
			table.Columns = append(table.Columns, column)
		case "string":
			column.Type = "VARCHAR"
			table.Columns = append(table.Columns, column)
		default:
			log.Println(t.Field(i).Type.Name())
			log.Println(t.Field(i).Type.Elem().Name())
		}
	}
}

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

func (table *Table) Test(s interface{}) {
	v := reflect.ValueOf(s)
	typeOfS := v.Type()

	for i := 0; i < v.NumField(); i++ {
		tag, _ := typeOfS.Field(i).Tag.Lookup("customsql")
		fmt.Printf("Field: %s\tValue: %v\tTag: %v\tType: %v\n", typeOfS.Field(i).Name, v.Field(i).Interface(), tag, typeOfS.Field(i).Type.Name())
	}
}

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

func GetTableName(i interface{}) string {
	name := ""
	row, ok := i.(Row)
	if ok {
		name = row.GetTableName()
	} else {
		if reflect.ValueOf(i).Kind() != reflect.Ptr {
			p := reflect.New(reflect.TypeOf(i))
			p.Elem().Set(reflect.ValueOf(i))
			row, ok = p.Interface().(Row)

			if ok {
				name = row.GetTableName()
			}
		}
	}
	if name == "" {
		name = getType(i)
	}
	/*switch i.(type) {
	case Row:
		name = i.(Row).GetTableName()
		log.Println(name + "1")
	case *Row:
		t := *(i.(*Row))
		name = t.GetTableName()
		log.Println(name + "2")
	default:
		name = getType(i)
		log.Println(name + "3")
	}*/
	return ToSnakeCase(name)
}

func getType(myvar interface{}) string {
	t := reflect.TypeOf(myvar)
	if t.Kind() == reflect.Ptr {
		return t.Elem().Name()
	}
	return t.Name()
}

func ToSnakeCase(str string) string {
	var matchFirstCap = regexp.MustCompile("[*]?([A-Za-z]+)([A-Z][a-z]+)")
	var matchAllCap = regexp.MustCompile("[*]?([a-z0-9])([A-Z])")

	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

func panicErr(err error) {
	if err != nil {
		panic(err)
	}
}

type Table struct {
	Name     string
	Columns  []Column
	FKeys    []FKey
	Uniq     []string
	Instance interface{}
}

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

type FKey struct {
	ColumnName      string
	ColumnValue     interface{}
	TableName       string
	TableColumnName string
	Type            reflect.Type
	FieldName       string
	IsNull          bool
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

func (table *Table) createTableSql() string {
	uniqLine := strings.Join(table.Uniq, ", ")
	if len(uniqLine) > 0 {
		uniqLine = ",\n UNIQUE (" + uniqLine + ")"
	}
	if len(table.Columns) == 0 {
		panicErr(errors.New("no table struct provided to create table"))
		return ""
	}

	var fKeySql = ""
	for _, val := range table.FKeys {
		fKeySql += val.toString()
		fKeySql += ",\n"
	}

	var columnsSql = ""
	for k, val := range table.Columns {
		columnsSql += val.toString()
		if k == len(table.Columns)-1 {
			continue
		}
		columnsSql += ",\n"
	}

	sqlReq := fmt.Sprintf(`	CREATE TABLE IF NOT EXISTS %s(
		%s%s%s
	)
	WITH (OIDS=FALSE);`,
		table.Name, fKeySql, columnsSql, uniqLine)
	//log.Println(sqlReq)
	return sqlReq
}

func (c *CORM) MovePosition(table Table) error {
	var newPosition int64
	var id int64
	var positionColumnName string
	for _, column := range table.Columns {
		if column.IsPosition {
			newPosition, _ = column.Value.(int64)
			positionColumnName = column.Name
			continue
		}
		if column.Name == "id" {
			id, _ = column.Value.(int64)
			continue
		}
	}
	if newPosition == 0 || id == 0 {
		return errors.New("no position or id column")
	}
	parentColumnName := ""
	var parentColumnValue int64
	//TODO: needs to take not a just first one
	for _, column := range table.FKeys {
		if column.IsNull {
			continue
		}
		parentColumnName = column.ColumnName
		parentColumnValue, _ = column.ColumnValue.(int64)
		break
	}
	if parentColumnName == "" {
		return errors.New("no parent column name")
	}

	tr, err := c.db.Begin()
	if err != nil {
		return err
	}
	defer tr.Rollback()

	var oldPosition int64
	if parentColumnValue == 0 {
		err = tr.QueryRow(fmt.Sprintf(`SELECT %s, %s FROM %s WHERE id = $1`, positionColumnName, parentColumnName, table.Name), id).Scan(&oldPosition, &parentColumnValue)
		if err != nil {
			return err
		}
	}
	if parentColumnValue == 0 {
		return errors.New("no parent column")
	}

	if oldPosition == 0 {
		err = tr.QueryRow(fmt.Sprintf(`SELECT %s FROM %s WHERE id = $1`, positionColumnName, table.Name), id).Scan(&oldPosition)
		if err != nil {
			return err
		}
	}
	if oldPosition == 0 {
		return errors.New("row not found")
	}
	pos1 := newPosition
	pos2 := newPosition + 1

	if oldPosition > newPosition {
		pos1 = newPosition - 1
		pos2 = newPosition
	}

	_, err = tr.Exec(fmt.Sprintf(`UPDATE %s SET %s = (%s + 1)*-1 WHERE %s = $1 AND %s > $2`, table.Name, positionColumnName, positionColumnName, parentColumnName, positionColumnName),
		parentColumnValue, pos1)
	if err != nil {
		return err
	}
	_, err = tr.Exec(fmt.Sprintf(`UPDATE %s SET %s = (%s)*-1 WHERE %s < 0`, table.Name, positionColumnName, positionColumnName, positionColumnName))
	if err != nil {
		return err
	}
	_, err = tr.Exec(fmt.Sprintf(`UPDATE %s SET %s = $2 WHERE id = $1`, table.Name, positionColumnName),
		id, pos2)
	if err != nil {
		return err
	}
	_, err = tr.Exec(fmt.Sprintf(`UPDATE %s SET %s = (%s - 1)*-1 WHERE %s = $1 AND %s > $2`, table.Name, positionColumnName, positionColumnName, parentColumnName, positionColumnName),
		parentColumnValue, oldPosition)
	if err != nil {
		return err
	}
	_, err = tr.Exec(fmt.Sprintf(`UPDATE %s SET %s = (%s)*-1 WHERE %s < 0`, table.Name, positionColumnName, positionColumnName, positionColumnName))
	if err != nil {
		return err
	}

	err = tr.Commit()
	if err != nil {
		return err
	}

	return err
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
