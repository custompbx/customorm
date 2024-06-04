package customorm

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/lib/pq"
	"log"
	"reflect"
	"strconv"
	"strings"
)

func (c *CORM) CreateTable(s interface{}) bool {
	table, err := c.GetTable(s)
	if err != nil {
		panicErr(err)
		return false
	}

	sqlReq, indexLines := table.createTableSql()
	if sqlReq == "" {
		panicErr(errors.New("cant create table " + table.Name))
	}
	_, err = c.db.Exec(sqlReq)

	panicErr(err)
	for _, s := range indexLines {
		_, err = c.db.Exec(s)
		panicErr(err)
	}

	return true
}

func (c *CORM) InsertRow(s interface{}) (int64, error) {
	table, err := c.GetTable(s)
	if err != nil {
		return 0, err
	}

	var id int64
	var names []string
	var values []interface{}
	var parentColumnName string
	var parentIndex int

	// Collect foreign key column names and values
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

	// Prepare position column SQL if necessary
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
	// Append position column SQL to SQL request and update names
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
					if t.Name != v.FieldName {
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
				if t.Name != v.FieldName {
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
		for _, v := range table.Columns {
			if v.FieldName != t.Name {
				continue
			}
			names = append(names, v.Name)
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

func getFilterParams(filter Filters, fieldName, name string, defaultValue interface{}) (string, interface{}, string, string) {
	var fName string
	if filter.Fields[fieldName].Flag {
		fName = fieldName
	} else if filter.Fields[name].Flag {
		fName = name
	}
	if fName == "" {
		return "", nil, "", ""
	}

	val := defaultValue
	if filter.Fields[fName].UseValue {
		val = filter.Fields[fName].Value
	}

	operand := OperandEqual
	postOperand := ""
	switch filter.Fields[fName].Operand {
	case OperandMore, OperandLess, OperandNotEqual:
		operand = filter.Fields[fName].Operand
	case OperandContains:
		operand = "LIKE '%' || "
		postOperand = " || '%'"
	case OperandIn:
		operand = "= ANY("
		postOperand = ")"
		switch v := val.(type) {
		case []int64:
			val = pq.Array(v)
		case []string:
			val = pq.Array(v)
		default:
			return "", nil, "", "wrong IN value"
		}
	}
	return fName, val, operand, postOperand
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
	var fnames []string
	var wheres []string
	var wheresArgs []interface{}
	var offset string
	var order string
	var positionColumnName string
	var primaryKeyColumnName string
	indirect := reflect.ValueOf(table.Instance)

	p := 1
	for _, v := range table.Columns {
		names = append(names, v.Name)
		fnames = append(fnames, v.FieldName)
		if v.IsPosition {
			positionColumnName = v.Name
		}
		fName, val, operand, postOperand := getFilterParams(filter, v.FieldName, v.Name, v.Value)
		if fName == "" {
			continue
		}

		line := v.Name + " " + operand + " $" + strconv.Itoa(p) + postOperand

		wheres = append(wheres, line)
		wheresArgs = append(wheresArgs, val)
		p++
	}

	for i := 0; i < indirect.NumField(); i++ {
		if indirect.Field(i).Kind() != reflect.Ptr {
			continue
		}
		t := indirect.Type().Field(i)
		for _, v := range table.FKeys {
			if t.Name != v.FieldName {
				continue
			}
			name := v.ColumnName
			if v.IsNull {
				//TODO: for now int only
				name = fmt.Sprintf("COALESCE(%s, 0)", v.ColumnName)
			}
			names = append(names, name)
			fnames = append(fnames, v.FieldName)
			//TODO: for one key only for now
			primaryKeyColumnName = name
			/*if isNil(v.ColumnValue) || v.ColumnValue == "" || v.ColumnValue == 0 || v.ColumnValue == false {
				continue
			}*/
			fName, val, operand, postOperand := getFilterParams(filter, v.FieldName, name, v.ColumnValue)
			if fName == "" {
				continue
			}
			line := v.ColumnName + " " + operand + " $" + strconv.Itoa(p) + postOperand

			wheres = append(wheres, line)
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
	var indexes = IndexesMap(fnames)
	for results.Next() {
		var ptrs = make([]interface{}, len(names))
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
					if t.Name != v.FieldName {
						continue
					}
					newValPkey := reflect.New(v.Type)
					f2 := newValPkey.Elem().FieldByName("Id")
					ptrs[indexes[v.FieldName]] = f2.Addr().Interface()
					f.Set(newValPkey)
				}
				continue
			}
			ptrs[indexes[t.Name]] = f.Addr().Interface()
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

func (c *CORM) GetDB() *sql.DB {
	return c.db
}
func IndexesMap(slice []string) map[string]int {
	res := make(map[string]int, len(slice))
	for i, v := range slice {
		res[v] = i
	}
	return res
}
