# Интеграционные тесты

Эта директория содержит интеграционные тесты на клиент к lb.

Для локальной разработки, можно запускать тесты двумя способами:

  * В distbuild.

```
ya make --dist -tt kikimr/public/sdk/go/persqueue/test
```

  * Поверх личного топика из [quickstart](https://logbroker.yandex-team.ru/docs/quickstart/).

```shell
env LOGBROKER_ENDPOINT=vla.logbroker.yandex.net go test -race -v a.yandex-team.ru/kikimr/public/sdk/go/persqueue/test
```