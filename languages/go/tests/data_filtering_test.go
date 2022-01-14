package oso_test

import (
 // "encoding/json"
  "os"
  "reflect"
  "testing"
  "strings"

  "gorm.io/gorm"
//  "gorm.io/gorm/schema"
  "gorm.io/driver/sqlite"

  oso "github.com/osohq/go-oso"
  "github.com/osohq/go-oso/internal/host"
  osoTypes "github.com/osohq/go-oso/types"
)

type GormAdapter struct {
  db *gorm.DB
  host *host.Host
}

func (a GormAdapter) sqlize(fc osoTypes.FilterCondition) (string, []interface{}) {
  args := []interface{}{}
  lhs := a.addSide(fc.Lhs, &args)
  rhs := a.addSide(fc.Rhs, &args)
  return lhs + " " + op(fc.Cmp) + " " + rhs, args
}

func op(c osoTypes.Comparison) string {
  switch c {
  case osoTypes.Eq:
    return "="
  case osoTypes.Neq:
    return "!="
  }
  return "IN"
}

func (a GormAdapter) addSide(d osoTypes.Datum, xs *[]interface{}) string {
  switch v := d.DatumVariant.(type) {
  case osoTypes.Projection:
    var fieldName string
    if v.FieldName == "" { // is this how none is returned to Go??
      fieldName = "ID"
    } else {
      fieldName = v.FieldName
    }
    tableName := a.tableName(v.TypeName)
    columnName := a.columnName(tableName, fieldName)
    return tableName + "." + columnName
  case osoTypes.Immediate:
    // not the best way to do this ...
    switch vv := v.Value.(type) {
    case Sign:
      *xs = append(*xs, vv.ID)
    case Person:
      *xs = append(*xs, vv.ID)
    case Planet:
      *xs = append(*xs, vv.ID)
    default:
      *xs = append(*xs, vv)
    }
  }
  return "?"
}

func (a GormAdapter) tableName(n string) string {
  return a.db.Config.NamingStrategy.TableName(n)
}

func (a GormAdapter) columnName(t string, n string) string {
  return a.db.Config.NamingStrategy.ColumnName(t, n)
}

func (a GormAdapter) BuildQuery(f *osoTypes.Filter) (interface{}, error) {
  models := map[string]interface{}{
    "Person": Person{},
    "Sign": Sign{},
    "Planet": Planet{},
  }
  model := models[f.Root]
  db := a.db.Table(a.tableName(f.Root)).Model(&model)

  for _, rel := range f.Relations {
    myTable := a.tableName(rel.FromTypeName)
    otherTable := a.tableName(rel.ToTypeName)
    myField, otherField := a.host.GetRelationFields(rel)
    myColumn := a.columnName(myTable, myField)
    otherColumn := a.columnName(otherTable, otherField)
    join := "INNER JOIN " + otherTable + " ON " + myTable + "." + myColumn + " = " + otherTable + "." + otherColumn
    db = db.Joins(join)
  }

  orSqls := []string{}
  args := []interface{}{}
  for _, orClause := range f.Conditions {
    andSqls := []string{}
    for _, andClause := range orClause {
      andSql, andArgs := a.sqlize(andClause)
      andSqls = append(andSqls, andSql)
      args = append(args, andArgs...)
    }

    if len(andSqls) > 0 {
      orSqls = append(orSqls, strings.Join(andSqls, " AND "))
    }
  }

  if len(orSqls) > 0 {
    sql := strings.Join(orSqls, " OR ")
    db = db.Where(sql, args...)
  }

  return db, nil
}

func (a GormAdapter) ExecuteQuery(x interface{}) ([]interface{}, error) {
  switch q := x.(type) {
  case *gorm.DB:
    switch (*q.Statement.Model.(*interface{})).(type) {
    case Person:
      v := make([]Person, 0)
      q.Distinct().Find(&v)
      w := make([]interface{}, 0)
      for _, i := range v {
        w = append(w, i)
      }
      return w, nil
    case Sign:
      v := make([]Sign, 0)
      q.Distinct().Find(&v)
      w := make([]interface{}, 0)
      for _, i := range v {
        w = append(w, i)
      }
      return w, nil
    case Planet:
      v := make([]Planet, 0)
      q.Distinct().Find(&v)
      w := make([]interface{}, 0)
      for _, i := range v {
        w = append(w, i)
      }
      return w, nil
    }
  }
  panic("a problem")
}

type Planet struct {
  gorm.Model
  Name string
}

type Sign struct {
  gorm.Model
  Name string
  Element string
  PlanetID uint
}

type Person struct {
  gorm.Model
  Name string
  SignID uint
}

func gormDb(dbFile string) *gorm.DB {
  os.Remove(dbFile)
  db, _ := gorm.Open(sqlite.Open(dbFile), &gorm.Config{})
  db.AutoMigrate(&Planet{})
  db.AutoMigrate(&Sign{})
  db.AutoMigrate(&Person{})

  db.Create(&Planet{Name: "mars"})    // 1
  db.Create(&Planet{Name: "venus"})   // 2
  db.Create(&Planet{Name: "mercury"}) // 3
  db.Create(&Planet{Name: "moon"})    // 4
  db.Create(&Planet{Name: "sun"})     // 5
  db.Create(&Planet{Name: "jupiter"}) // 6
  db.Create(&Planet{Name: "saturn"})  // 7
  db.Create(&Planet{Name: "pluto"})   // 8

  db.Create(&Sign{Name: "aries", Element: "fire", PlanetID: 1})       // 1
  db.Create(&Sign{Name: "taurus", Element: "earth", PlanetID: 2})     // 2
  db.Create(&Sign{Name: "gemini", Element: "air", PlanetID: 3})       // 3
  db.Create(&Sign{Name: "cancer", Element: "water", PlanetID: 4})     // 4
  db.Create(&Sign{Name: "leo", Element: "fire", PlanetID: 5})         // 5
  db.Create(&Sign{Name: "virgo", Element: "earth", PlanetID: 3})      // 6
  db.Create(&Sign{Name: "libra", Element: "air", PlanetID: 2})        // 7
  db.Create(&Sign{Name: "scorpio", Element: "water", PlanetID: 1})    // 8
  db.Create(&Sign{Name: "sagittarius", Element: "fire", PlanetID: 6}) // 9
  db.Create(&Sign{Name: "capricorn", Element: "earth", PlanetID: 7})  // 10
  db.Create(&Sign{Name: "aquarius", Element: "air", PlanetID: 7})     // 11
  db.Create(&Sign{Name: "pisces", Element: "water", PlanetID: 6})     // 12

  db.Create(&Person{Name: "robin", SignID: 8})
  db.Create(&Person{Name: "pat", SignID: 2})
  db.Create(&Person{Name: "mercury", SignID: 6})
  db.Create(&Person{Name: "terry", SignID: 7})
  db.Create(&Person{Name: "chris", SignID: 11})
  db.Create(&Person{Name: "leo", SignID: 5})
  db.Create(&Person{Name: "eden", SignID: 4})
  db.Create(&Person{Name: "dakota", SignID: 10})
  db.Create(&Person{Name: "charlie", SignID: 1})
  db.Create(&Person{Name: "alex", SignID: 3})
  db.Create(&Person{Name: "sam", SignID: 12})
  db.Create(&Person{Name: "avery", SignID: 9})

  return db
}

func testOso() oso.Oso {
  o, _ := oso.NewOso()
  o.SetDataFilteringAdapter(GormAdapter{gormDb("test.sqlite"), o.GetHost()})
  o.RegisterClassWithNameAndFields(reflect.TypeOf(Person{}), nil, "Person", map[string]interface{}{
    "Name": "String",
    "ID": "Integer",
    "SignID": "Integer",
    "Sign": osoTypes.Relation {
      Kind: "one",
      OtherType: "Sign",
      MyField: "SignID",
      OtherField: "ID",
    },
  })
  o.RegisterClassWithNameAndFields(reflect.TypeOf(Sign{}), nil, "Sign", map[string]interface{}{
    "Name": "String",
    "Element": "String",
    "ID": "Integer",
    "PlanetID": "Integer",
    "People": osoTypes.Relation {
      Kind: "many",
      OtherType: "Person",
      MyField: "ID",
      OtherField: "SignID",
    },
    "Planet": osoTypes.Relation {
      Kind: "one",
      OtherType: "Planet",
      MyField: "PlanetID",
      OtherField: "ID",
    },
  })
  o.RegisterClassWithNameAndFields(reflect.TypeOf(Planet{}), nil, "Planet", map[string]interface{}{
    "Name": "String",
    "ID": "Integer",
    "Signs": osoTypes.Relation {
      Kind: "many",
      OtherType: "Sign",
      MyField: "ID",
      OtherField: "PlanetID",
    },
  })
  return o
}

func TestFieldCmpRelField(t *testing.T) {
  o := testOso()
  o.LoadString("allow(_, _, p: Person) if p.Name = p.Sign.Name;")
  res, err := o.AuthorizedResources("gwen", "read", "Person")
  if err != nil {
    t.Error(err.Error())
  }
  if len(res) != 1 {
    t.Errorf("Expected 1 result, got %d", len(res))
  }
  switch p := res[0].(type) {
  case Person:
    if p.Name != "leo" {
      t.Errorf("Expected leo, got %s", p.Name)
    }
  default:
    t.Errorf("Expected a Person, got %v", p)
  }
}