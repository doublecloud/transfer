# `typesystem`

Package `typesystem` enables Data Transfer to be compatible with different type system versions and provides definitions of a Data Transfer type system.

See [CHANGELOG.md](./CHANGELOG.md) for details on what each fallback does.

A version of a type system is a set of rules of conversion of specific source database types to Data Transfer types, as well as rule of their conversion to specific target database types. Type system is thus only about heterogenous transfers.

This package consists of the typesystem and document methods which define typesystem rules (the mentioned rules of conversion) and the fallback facility methods.

Fallbacks enable users of older versions of transfers (with typesystem version, a property of the transfer, set to some value different from the "current" one) to convert types differently than the newly created transfers do. This feature enables to introduce some non-backwards-compatible changes to Data Transfer without any extra settings.

Use `AddFallbackSource` and `AddFallbackTarget` methods to add fallbacks. For example, a fallback for PostgreSQL source is added as follows:

```go
// In file `pkg/dataagent/pg/fallback_date_as_string.go`
func init() {
    typesystem.AddFallbackSource(typesystem.Fallback{
        To:           1,
        ProviderType: abstract.ProviderTypePostgreSQL,
        Function: func(ci *abstract.ChangeItem) (*abstract.ChangeItem, error) {
            if !ci.IsRowEvent() {
                switch ci.Kind {
                case abstract.InitTableLoad, abstract.DoneTableLoad:
                    // perform fallback
                default:
                    return ci, nil
                }
            }

            for i := 0; i < len(ci.TableSchema); i++ {
                switch ci.TableSchema[i].DataType {
                case schema.TypeDate.String(), schema.TypeDatetime.String(), schema.TypeTimestamp.String():
                    ci.TableSchema[i].DataType = schema.TypeString.String()
                default:
                    // do nothing
                }
            }
            return ci, nil
        },
    })
}

// ...

// In `typesystem` package:
const LatestVersion int = 2 // previously was `1`
```

See function descriptions and actual use examples for extra details.

Each type system change (update) requires an increment of the global "latest" type system version constant, located inside this package.
