# log package

`log` package provide logging internal ydb-go-sdk events using `library/go/core/log.Logger`

## Usage for usual SDK
```go
import (
  "github.com/ydb-platform/ydb-go-sdk/v3"
  "github.com/ydb-platform/ydb-go-sdk/v3/trace"

  "a.yandex-team.ru/library/go/core/log"
  ydbLog "a.yandex-team.ru/library/go/yandex/ydb/log"
)

func main() {
  var log log.Logger // init outside this code

  db, err := ydb.New(
    context.Background(),
    ydb.MustConnectionString(connection),
    ydbLog.WithTraces(
      log,
      trace.DetailsAll,
    ),
  )
  // work with db
}
```

## Usage for Federative topic SDK

