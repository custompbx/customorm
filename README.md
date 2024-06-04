Here's an improved version of your README:

# CustomORM

CustomORM is not a full-fledged ORM but rather a collection of functions designed to generate and execute SQL queries based on specific struct definitions. It is tailored for PostgreSQL databases.

## Features

- Generate and execute SQL queries based on struct definitions
- Designed to work seamlessly with PostgreSQL databases
- Support for table creation, insertion, updating, deletion, and querying

## Usage

### Struct Definitions

Define your structs with tags specifying SQL constraints and properties:

```go
type Domain struct {
	Id      int64  `json:"id" customsql:"pkey:id;check(id <> 0)"`
	Enabled bool   `json:"enabled" customsql:"enabled;default=TRUE"`
	Name    string `json:"name" customsql:"name;unique;check(name <> '')"`
}

func (d *Domain) GetTableName() string {
    return "domains"
}

type DomainUser struct {
	Id      int64    `json:"id" customsql:"pkey:id;check(id <> 0)"`
	Position int64    `json:"position" customsql:"position;position"`
	Enabled bool     `json:"enabled" customsql:"enabled;default=TRUE"`
	Name    string   `json:"name" customsql:"name;unique_1;check(name <> '')"`
	Parent  *Domain  `json:"parent" customsql:"fkey:parent_id;unique_1;check(parent_id <> 0)"`
}

func (d *DomainUser) GetTableName() string {
    return "domain_users"
}
```
The tags `unique` and `index` can be grouped using an underscore, such as `unique_1`. To set the order within a group, add a suffix after a hyphen, for example, `index_1-1` and `index_1-2`.

### Creating Tables

```go
corm := customorm.Init(db)
corm.CreateTable(&Domain{})     // Returns bool
corm.CreateTable(&DomainUser{}) // Returns bool
```

### Inserting Rows

```go
corm := customorm.Init(db)
corm.InsertRow(&DomainUser{
    Name:    "UserName",
    Parent:  &altStruct.Domain{Id: parentId},
    Enabled: true,
}) // Returns int64, error
```

### Updating Rows

```go
corm := customorm.Init(db)
corm.UpdateRow(&DomainUser{
    Id:   1,
    Name: "NewUserName",
}, true, map[string]bool{"Name": true}) // Returns error
```

### Deleting Rows

```go
corm := customorm.Init(db)
corm.DeleteRowById(&DomainUser{Id: 1})           // Returns error
corm.DeleteRowByArgId(&DomainUser{}, 1)          // Returns error
corm.DeleteRows(&DomainUser{Enabled: false}, map[string]bool{"Enabled": true}) // Returns error
```

### Querying Rows

```go
corm := customorm.Init(db)
corm.GetDataAll(&DomainUser{}, true) // Returns interface{}, error

filter := customOrm.Filters{
    Fields: map[string]FilterFields{
        "Name": {
            Flag:      true, // Use this filter
            UseValue:  false, // Use value from struct or from value field below
            Value:     nil, // Can use this value instead of value from struct
            Operand:   "", // Operand to compare with value of (<,>,=,CONTAINS,IN) default "="
        },
    },
    Order: customOrm.Order{
        Desc:   true, // Sort DESC
        Fields: []string{"Id"}, // Sort by field names
    },
    Limit:  10,
    Offset: 0,
    Count:  false, // If true, returns count(*) instead of fields
}

corm.GetDataByValue(&DomainUser{Id: 1}, filter, false) // Returns interface{}, error
```

Feel free to adjust and expand upon these examples to suit your specific use cases.