# This repository was deprecated. 
# Latest ydb-go-sdk develops in https://github.com/ydb-platform/ydb-go-sdk

# ydb

> YDB API client written in Go.

[godoc](https://godoc.org/github.com/yandex-cloud/ydb-go-sdk/v2/)

## Table of contents
1. [Overview](#Overview)
2. [About semantic versioning](#SemVer)
3. [Prerequisites](#Prerequisites)
4. [Installation](#Install)
5. [Usage](#Usage)
    * [Native API](#Native)
    * [Database/sql API](#database-sql)
6. [Credentials](#Credentials)
7. [Generating code](#ydbgen)
    * [Installation](#ydbgen-install)
    * [Usage](#ydbgen-usage)
    * [Configuration](#ydbgen-configuration)
      * [Binary flags](#ydbgen-binary-flags)
      * [Comment markers](#ydbgen-comment-markers)
      * [Struct tags and values](#ydbgen-struct-tags-and-values)
    * [Customization](#ydbgen-customization)
      * [Optional types](#ydbgen-customization-optional-types)
      * [Dealing with `time.Time`](#ydbgen-customization-time)
      * [Dealing with container types](#ydbgen-customization-container)
8. [Examples](#examples)

## Overview <a name="Overview"></a>

Currently package ydb provides `scheme` and `table` API client implementations for `YDB`.

## About semantic versioning <a name="SemVer"></a>

We follow the **[SemVer 2.0.0](https://semver.org)**. In particular, we provide backward compatibility in the `MAJOR` releases. New features without loss of backward compatibility appear on the `MINOR` release. In the minor version, the patch number starts from `0`. Bug fixes and internal changes are released with the third digit (`PATCH`) in the version.

There are, however, some changes with the loss of backward compatibility that we consider to be `MINOR`:
* extension or modification of internal `ydb-go-sdk` interfaces. We understand that this will break the compatibility of custom implementations of the `ydb-go-sdk` internal interfaces. But we believe that the internal interfaces of `ydb-go-sdk` are implemented well enough that they do not require custom implementation. We are working to ensure that all internal interfaces have limited access only inside `ydb-go-sdk`.
* major changes to (including removal of) the public interfaces and types that have been previously exported by `ydb-go-sdk`. We understand that these changes will break the backward compatibility of early adopters of these interfaces. However, these changes are generally coordinated with early adopters and have the concise interfacing with `ydb-go-sdk` as a goal.

Internal interfaces outside from `internal` directory are marked with comment such as
```
// Warning: only for internal usage inside ydb-go-sdk
```

We publish the planned breaking `MAJOR` changes:
* via the comment `Deprecated` in the code indicating what should be used instead
* through the file [`NEXT_MAJOR_RELEASE.md`](#NEXT_MAJOR_RELEASE.md)

## Prerequisites <a name="Prerequisites"></a>

Requires Go 1.13 or later.

## Installation <a name="Installation"></a>

```
go get -u github.com/yandex-cloud/ydb-go-sdk/v2
```

## Usage <a name="Usage"></a>

## Native usage <a name="Native"></a>

The straightforward example of querying data may looks similar to this:

```go
// Determine timeout for connect or do nothing
connectCtx, cancel := context.WithTimeout(ctx, time.Second)
defer cancel()

// connect package helps to connect to database, returns connection object which
// provide necessary clients such as table.Client, scheme.Client, etc.
// All manipulations with the connection could be done without the connect package
db, err := connect.New(
    connectCtx,
    connect.MustConnectionString(
    	"grpcs://ydb-ru.yandex.net:2135/?database=/ru/home/username/db",
    ),
)
if err != nil {
    return fmt.Errorf("connect error: %w", err)
}
defer db.Close()

// Create session for execute queries
session, err := db.Table().CreateSession(ctx)
if err != nil {
    // handle error
}
defer session.Close(ctx)

// Prepare transaction control for upcoming query execution.
// NOTE: result of TxControl() may be reused.
txc := table.TxControl(
    table.BeginTx(table.WithSerializableReadWrite()),
    table.CommitTx(),
)

// Execute text query without preparation and with given "autocommit"
// transaction control. That is, transaction will be commited without
// additional calls. Notice the "_" unused variable – it stands for created
// transaction during execution, but as said above, transaction is commited
// for us and we do not want to do anything with it.
_, res, err := session.Execute(ctx, txc,
    `--!syntax_v1
        DECLARE $mystr AS Utf8?;
        SELECT 42 as id, $mystr as mystr
    `,
    table.NewQueryParameters(
        table.ValueParam("$mystr", ydb.OptionalValue(ydb.UTF8Value("test"))),
    ),
)
if err != nil {
    return err // handle error
}
// Scan for received values within the result set(s).
// res.Err() reports the reason of last unsuccessful one.
var (
    id    int32
    myStr *string //optional value
)
for res.NextResultSet("id", "mystr") {
    for res.NextRow() {
        // Suppose our "users" table has two rows: id and age.
        // Thus, current row will contain two appropriate items with
        // exactly the same order.
        err := res.Scan(&id, &myStr)

        // Error handling.
        if err != nil {
            return err
        }
        // do something with data
        fmt.Printf("got id %v, got mystr: %v\n", id, *myStr)
    }
}
if res.Err() != nil {
    return res.Err() // handle error
}
```

This example can be tested as `ydb/example/from_readme`

YDB sessions may become staled and appropriate error will be returned. To
reduce boilerplate overhead for such cases `ydb-go-sdk` provides generic retry logic:

```go
	var res *table.Result
	// Retry() will call given OperationFunc with the following invariants:
	//  - previous operation failed with retriable error;
	//  - number of retries is under the limit (default to 10, see table.Retryer docs);
	//
	// Note that in case of prepared statements call to Prepare() must be made
	// inside the Operation body.
	err = table.Retry(ctx, db.Table().Pool(),
		table.OperationFunc(func(ctx context.Context, s *table.Session) (err error) {
			res, err = s.Execute(...)
			return
		}),
	)
```

That is, instead of manual creation of `table.Session`, we give a
`SessionPool` such responsibility. It holds instances of active sessions and
"pings" them periodically to keep them alive.

See `table.Retryer` docs for more information about retry options.

### Database/sql driver <a name="database-sql"></a>

There is a `database/sql` driver for the sql-based applications or those which
by some reasons need to use additional layer of absctraction between user code
and storage backend.

For more information please see the docs of `ydbsql` package which provides
`database/sql` driver implementation.

## Credentials <a name="Credentials"></a>

There are different variants to get `ydb.Credentials` object to get authorized.
Usage examples can be found [here](example/internal/cli/driver.go) at `func credentials(...) ydb.Credentials`.

## Generating code <a name="ydbgen"></a>

### Overview <a name="ydbgen-overview"></a>

There is lot of boilerplate code for scanning values from query result and for
passing values to prepare query. There is an **experimental** tool named
`ydbgen` aimed to solve this.

#### Installation ydbgen <a name="ydbgen-install"></a>

```
go get -u github.com/yandex-cloud/ydb-go-sdk/v2/cmd/ydbgen
```

### Usage ydbgen <a name="ydbgen-usage"></a>

`ydbgen` helps to generate such things:
- scanning values from result into a struct or slice of structs;
- building query parameters from struct
- building ydb's struct value from struct
- building ydb's list value from slice of structs

The very short example could be like this:

```go
package somepkg

//go:generate ydbgen

//ydb:gen scan
type User struct {
	Name string
	Age  int32
}
```

After running `go generate path/to/somepkg/dir` file with suffix `_ydbgen.go`
will be generated. It will contain method `Scan()` for `User` type, as
requested in the *generate comment*.

### Configuration <a name="ydbgen-configuration"></a>

Generation may be configured at three levels starting from top:
- ydbgen binary flags (package level)
- comment markers right before generation object in form of `//ydb:set
  [key1:value1 [... keyN:valueN]]` (type level)
- [struct tags](https://golang.org/ref/spec#Tag) (field level)

Each downstream level overrides options for its context.

For example, this code will generate all possible code for `User` struct with
field `Age` type mapped to **non-optional** type, because the lowermost
configuration level (which is struct tag) defines non-optional `uint32` type:

```go
//go:generate ydbgen -wrap optional

//ydb:gen
//ydb:set wrap:none
type User struct {
	Age int32 `ydb:"type:uint32,column:user_age"`
}
```


#### Binary flags <a name="ydbgen-binary-flags"></a>

 Flag   | Value      | Default | Meaning
--------|:----------:|:-------:|---------
 `wrap` | `optional` | +       | Wraps all mapped field types with optional type if no explicit tag is specified.
 `wrap` | `none`     |         | No wrapping performed.
 `seek` | `column`   | +       | Uses res.SeekItem() call to find out next field to scan.
 `seek` | `position` |         | Uses res.NextItem() call to find out next field to scan.

#### Comment markers options <a name="ydbgen-comment-markers"></a>

Options for comment markers are similar to flags, except the form of
serialization.

#### Struct tags and values overview <a name="ydbgen-struct-tags-and-values"></a>

 Tag      | Value    | Default | Meaning
----------|:--------:|:-------:|---------
 `type`   | `T`      |         | Specifies which ydb primitive type must be used for this field.
 `type`   | `T?`     |         | The same as above, but wraps T with optional type.
 `conv`   | `safe`   | +       | Prepares only safe type conversions. Fail generation if conversion is not possible.
 `conv`   | `unsafe` |         | Prepares unsafe type conversions too.
 `conv`   | `assert` |         | Prepares safety assertions before type conversion.
 `column` | `string` |         | Maps field to this column name.

Also the shorthand tags are possible: when using tag without `key:value` form,
tag with `-` value is interpreted as field must be ignored; in other way it is
interpreted as the column name.

### Customization <a name="ydbgen-customization"></a>

There are few additional options existing for flexibility purposes.

#### Optional Types <a name="ydbgen-customization-optional-types"></a>

Previously only basic Go types were mentioned as ones that able to be converted
to ydb types. But it is possible generate code that maps defined type to YDB
type (actually to basic Go type and then to YDB type). To make so, such type
must provide two methods (when generation both getter and setters) – `Get() (T,
bool)` and `Set(T)`, where `T` is a basic Go type, and `bool` is a flag that
indicates that value defined.

```go
//go:generate ydbgen

//ydb:gen
type User struct {
	Name OptString
}

type OptString struct {
	Value   string
	Defined bool
}

func (s OptString) Get() (string, bool) {
	return s.Value, s.Defined
}
func (s *OptString) Set(v string) {
	*s = OptString{
		Value:   v,
		Defined: true,
	}
}
```

There is special package called `ydb/opt` for this purposes:

```go
package main

import "github.com/yandex-cloud/ydb-go-sdk/v2/opt"

//go:generate ydbgen

//ydb:gen
type User struct {
	Name opt.String
}
```

#### Dealing with `time.Time` <a name="ydbgen-customization-time"></a>

There is additional feature that makes it easier to work with `time.Time`
values and their conversion to YDB types:

```go
//go:generate ydbgen

//ydb:gen
type User struct {
	Updated time.Time `ydb:"type:timestamp?"`
}
```

#### Dealing with container types <a name="ydbgen-customization-container"></a>

`ydbgen` supports scanning and serializing container types such as `List<T>` or `Struct<T>`.

```go
//go:generate ydbgen

//ydb:gen
type User struct {
	Tags []string `ydb:"type:list<string>"`
}
```

Example above will interpret value for `tags` column (or 0-th item, depending
on the `seek` mode) as `List<String>`.

Note that for `String` type this is neccessary to inform `ydbgen` that it is
not a container by setting `type` field tag.

> For more info please look at `ydb/examples/generation` folder.

## Examples <a name="examples"></a>

More examples are listed in `ydb/examples` directory.

