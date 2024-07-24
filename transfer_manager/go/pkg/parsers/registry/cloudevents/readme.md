## Описание парсера

У ребят вот есть описание в вики: https://wiki.yandex-team.ru/ecom-ridetech/x-func-tech/spravochnik-konvencijj/cloudevents/

Примерно то же самое описано в первоначальном тикете-фичареквесте: [TM-6043]

Есть опенсурсный проект https://cloudevents.io/ & https://github.com/cloudevents

Этот опенсурс-проект задает формат прото-мессаджей (https://github.com/cloudevents/spec/blob/main/cloudevents/formats/cloudevents.proto) (backup copy: https://paste.yandex-team.ru/d4786124-c538-46e0-aab3-4def6ef2e889):

```protobuf
syntax = "proto3";

package io.cloudevents.v1;

import "google/protobuf/any.proto";
import "google/protobuf/timestamp.proto";

message CloudEvent {

  // -- CloudEvent Context Attributes

  // Required Attributes
  string id = 1;
  string source = 2; // URI-reference
  string spec_version = 3;
  string type = 4;

  // Optional & Extension Attributes
  map<string, CloudEventAttributeValue> attributes = 5;

  // -- CloudEvent Data (Bytes, Text, or Proto)
  oneof  data {
    bytes binary_data = 6;
    string text_data = 7;
    google.protobuf.Any proto_data = 8;
  }

  /**
   * The CloudEvent specification defines
   * seven attribute value types...
   */

  message CloudEventAttributeValue {
    oneof attr {
      bool ce_boolean = 1;
      int32 ce_integer = 2;
      string ce_string = 3;
      bytes ce_bytes = 4;
      string ce_uri = 5;
      string ce_uri_ref = 6;
      google.protobuf.Timestamp ce_timestamp = 7;
    }
  }
}

message CloudEventBatch {
  repeated CloudEvent events = 1;
}
```

Что ребята из маркета сделали поверх этого дела:
* в мапе `attributes` ключ `dataschema` (тип oneof из `CloudEventAttributeValue`: `ce_uri`, protobuf:`string`) - задает URI до схемы, которой закодирован data (пример значения: `http://localhost:8081/schemas/ids/2`). Также мы его сохраняем в YT поле `dataschema`.
* в мапе `attributes` ключ `time` (тип oneof из `CloudEventAttributeValue`: `ce_timestamp`, protobuf:`google.protobuf.Timestamp`). Мы его сохраняем в YT поле `time`.
* в мапе `attributes` ключ `subject` (тип oneof из `CloudEventAttributeValue`: `ce_string`, protobuf:`string`). Мы его сохраняем в YT поле `subject`.
* в поле `proto_data` (`google.protobuf.Any proto_data = 8;`), задается поле `type_url`, которое сообщает какой именно мессаджем из прото-файла (полученному при помощи поля `dataschema`) получен. По сути это механизм, альтернативный механизму `Message Indexes`.

В итоге мы порождаем changeItem со следующей схемой (все поставки: lb(parser:cloud_events)->yt_dyn):

```go
columns := []abstract.ColSchema{
	newColSchema("id", ytschema.TypeString, true),          // field from message 'CloudEvent'
	newColSchema("source", ytschema.TypeString, false),     // field from message 'CloudEvent'
	newColSchema("type", ytschema.TypeString, false),       // field from message 'CloudEvent'
	newColSchema("dataschema", ytschema.TypeString, false), // filled from attributes
	newColSchema("subject", ytschema.TypeString, false),    // filled from attributes
	newColSchema("time", ytschema.TypeTimestamp, false),    // filled from attributes
	newColSchema("payload", ytschema.TypeAny, false),       // decoded from field 'proto_data' via SchemaRegistry (by field 'dataschema') with message, who configured via subfield 'type_url' of field 'proto_data'
}
```


## Полезные ссылки и заметки:

* `transfer_manager/go/pkg/parsers/registry/cloudevents/engine` - сам парсер
* `transfer_manager/go/tests/e2e/kafka2yt/cloudevents` - e2e-тест
* поскольку в рецепте SchemaRegistry поднимается на рандомном порту - мы в тестах пересобираем прото-мессадж с исправленным `dataschema`
* unparsed не будет содержать символа `@` в tableName, и т.о. таблица в ыте для анпарседа - создастся ([TM-6203])
* в парсере есть приватная настройка `PasswordFallback` - SchemaRegistry попробует сначала `Password`, а в случае неуспеха: `PasswordFallback`. Сделано для бесшовной миграции между разными SchemaRegistry ([SCHEMAREGISTRY-61])
* 404 от SchemaRegistry мы отправляем в unparsed (такое случается у ребят при разработке)


## История развития парсера


### [TM-6043] Добавить с confluent schema registry protobuf + сделать обёртку для CloudEvents

В рамках этого тикета сделали первую имплементацию


### [DTSUPPORT-3292] Невалидное поле time после трансфера

В YT поле `time` должно заполняться значением, полученным из аттрибута `time` (тип `ce_timestamp`)


### [TM-6438] Data Transfer пытается создать dyn-таблицы с недопустимыми именами

В YT поле `subject` должно заполняться значением, полученным из аттрибута `subject` (тип `ce_string`)

### [TM-7131] parser_cloud_events должен рассматривать 404 при получении схемы из SchemaRegistry как unparsed

Сделали это поведением по умолчанию, без возможности переопределять


### [TM-6209] CloudEvents parser

Тут пользователь столкнулся с необходимостью Message Indexes - мы вспомнили что договаривались что для маркета это не нужно, заодно вспомнили что такое Message Indexes в прото & wire format


### [TM-7214] CloudEvents - добавить обработку google.protobuf.Any

Тут реализована возможность указывать конкретный мессадж из прото-схемы (получаемой из SchemaRegistry) в поле `proto_data` (`google.protobuf.Any proto_data = 8;`), вместо механизма Message Indexes (см TM-6209)

Описание типа данных `google.protobuf.Any`

https://github.com/protocolbuffers/protobuf/blob/main/src/google/protobuf/any.proto#L128 (backup copy: https://paste.yandex-team.ru/431c4317-9127-427e-afa2-e6b142113bc0)

```protobuf
message Any {
  string type_url = 1;
  bytes value = 2;
}
```

Вот в поле type_url ребята помещают строчку `type.googleapis.com/ru.yandex.market.soc.shtnc.shtncshotbe.StartInitProcessRequest`, а мы из прото-описания, полученного из SchemaRegistry, извлечем описание мессаджа: `package ru.yandex.market.soc.shtnc.shtncshotbe` - `message StartInitProcessRequest`


### [DTSUPPORT-3069] Fixed cloud events parser

Тут пофиксили прото-парсинг
