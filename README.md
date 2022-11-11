# customorm
Not a real ORM, just a bunch of functions to generate and run SQL queries by specific structs. Functions receive and return interfaces and can panic.
### examples
#### structs
- fields Id is mandatory (with pkey tag)  
- `GetTableName` method can be used to rewrite table name
```go
type Domain struct {
	Id          int64   `json:"id" customsql:"pkey:id;check(id <> 0)"`
	Enabled     bool    `json:"enabled" customsql:"enabled;default=TRUE"`
	Name        string  `json:"name" customsql:"name;unique;check(name <> '')"`
}

func (w *Domain) GetTableName() string {
    return "domains"
}

type DomainUser struct {
	Id          int64   `json:"id" customsql:"pkey:id;check(id <> 0)"`
	Position    int64   `json:"position" customsql:"position;position"`
	Enabled     bool    `json:"enabled" customsql:"enabled;default=TRUE"`
	Name        string  `json:"name" customsql:"name;unique;check(name <> '')"`
	Parent      *Domain `json:"parent" customsql:"fkey:parent_id;unique;check(parent_id <> 0)"`
}

func (w *DomainUser) GetTableName() string {
    return "domain_users"
}
```
#### create table
```go
corm := customorm.Init(db)
corm.CreateTable(&Domain{}) // return bool
corm.CreateTable(&DomainUser{}) // return bool
```
#### insert row
```go
corm := customorm.Init(db)
corm.InsertRow(&DomainUser{
    Name: "UserName",
    Parent: &altStruct.Domain{Id: parentId},
    Enabled: true,
}) // return int64, error
```
#### update row/rows
```go
corm := customorm.Init(db)
corm.UpdateRow(&DomainUser{
	    Id: 1,
	    Name: "NewUserName"),
    },
    true,
    map[string]bool{"Name": true},
) // return error
```
#### delete row/rows
```go
corm := customorm.Init(db)
corm.DeleteRowById(&DomainUser{Id: 1}) // return error
corm.DeleteRowByArgId(&DomainUser{}, 1) // return error
corm.DeleteRows(&DomainUser{Enabled: false}, map[string]bool{"Enabled": true}) // return error
```
#### select row/rows
```go
corm := customorm.Init(db)
corm.GetDataAll(&DomainUser{}, true) // return interface{}, error

filter := customOrm.Filters{
            Fields map[string]FilterFields{
                "Name": {
                    Flag: true,             // use this filter
                    UseValue false,         // use value from struct or from value field below
                    Value nil,              // can use this value instead value from struct
                    Operand: "",            // operand to compare with value of (<,>,=,CONTAINS,IN) default "="
                },
            },
            Order customOrm.Order{
                Desc: true,                 // sort DESC
                Fields: []string{"Id"}      // sort by fields names
            }
            Limit: 10,
            Offset: 0,
            Count: false,                   // if true returns count(*) instead of fields
}

corm.GetDataByValue(
    &DomainUser{Id: 1},
    filter, 
    false,                                  // return interface will contain slice of results(map if true)
) // return interface{}, error
```
