# Специфика нашей имплементации debezium-протокола и оригинальной имплементации

## Arrays

Дебезиум (версии 1.8) в документации ничего не пишет про массивы.
Тем не менее дебезиум поддерживает массивы, с некоторыми оговорками:
- Массивы не всех типов поддержаны (TODO - написать какие не поддержаны)
- Некоторые типы переносятся с потерей модификатора типа (это можно легко посмотреть - все использования флага intoArr)
  - time with time zone
  - time without time zone
  - timestamp without time zone
- Многомерные массивы не поддерживаются (поддерживается один вырожденный кейс - когда все размерности кроме одной равны единице)

## json

json в промежуточном представлении (на пути от источника к приемнику) сейчас присутствует в десериализованном виде - поэтому мы имеем ряд артефактов.

### Мы теряем оригинальные отступы
```json
{"k1":1,   "k2":2}
```
перенесется как
```json
{"k1": 1, "k2": 2}
```

## wkb

Постгресовый point содержит какое-то wkb, хз как мне это воспроизводить:

```json
{"x":23.4,"y":-44.5,"wkb":"AQEAAABmZmZmZmY3QAAAAAAAQEbA","srid":null}
```
перенесётся как:
```json
{"x":23.4,"y":-44.5,"wkb":"","srid":null}
```

## sequence
Где-то между 1.2 и 1.8 в after появилось поле "sequence".
TODO - что там хранится-то?
Когда-нибудь поддержим. абсолютно не выглядит критичным

## serial
Где-то между 1.2 и 1.8 для полей serial в описании схемы появилось
```
default:0
```
когда-нибудь поддержим. абсолютно не выглядит критичным

И какие-то еще костыли были нужны раньше для serial в дебезиуме, а потом они это починили

## float

Совершенно неважная мелочь - у нас флоты записываются с маленькой 'e', а в дебезиуме с большой 'E'

data-transfer:
```json
{"real_": 1.45e-10}
```
debezium:
```json
{"real_": 1.45E-10}
```

## raw bytes

Старые версии дебезиума для сырых буферов для base64 захватывали больше байт чем нужно, выравнивая разницу нулями
Потом пофиксили, но вот у меня осталась старая имплементация, реализующая этот баг: changeItemsBitsToDebeziumWA

## updates changing pkey

Это скорее заметка для нас (кто мало знаком с дебезиумом)

Если прилетает update изменяющий первичный ключ - генерится 3 дебезиум-события:
- delete old row
- tombstone event
- insert new row

Поэтому у нас не всегда получится гонять changeItem->debeziumMsg->changeItem с полным соответствием входа выходу. Но пока что это никогда и не было нужно.

Также любой delete порождает tombstone event

## Поле "snapshot"

Нотация дебезиума:
- true - Record is from snapshot is not the last one.
- last - Record is from snapshot is the last record generated in snapshot phase
- false - Record is from streaming phase
- incremental - Record is from incremental snapshot window

У нас сейчас поддержано только true/false - нужно поддержать last, ну и если поддержим инкрементальные снапшоты - добавить и их

## Остальные особенности дебезиума

- На каждую таблицу по дефолту создается по топику (имя топика: serverName.schemaName.tableName)
```
serverName - The logical name of the connector, as specified by the database.server.name configuration property
```
- Пишет снапшот базы в кафка-топик (если так настроено): "op":"r"
- В оригинальном дебезиуме есть аналог нашего upload - "Ad hoc snapshots" (это re-run снапшота, и используются signaling tables). Любопытно что запускается это через служебные таблицы дебезиума - ты можешь в них записать сигнал, который сообщит дебезиуму что нужно снять адхок-снапшот
- PG: Для pg дебезиум умеет работать с тремя плагинами: wal2json/decoderbufs/pgoutput. Начиная с 10го постгреса можно работать без плагина
```
As of PostgreSQL 10+, there is a logical replication stream mode, called pgoutput that is natively supported by PostgreSQL. This means that a Debezium PostgreSQL connector can consume that replication stream without the need for additional plug-ins
```
- PG: Судя по доке дебезиума для pg - там переключение snapshot->replication с exactly once
```
After the connector completes the snapshot, it continues streaming changes from the exact point at which the snapshot was made
```
- PG: DDL так же нету в wal и соответственно нет в топиках
- PG: Репликация так же доступна только с мастеров
- PG: Оригинальный дебезиум: "Debezium currently supports databases with UTF-8 character encoding only"
- PG: Оригинальный дебезиум не переживает переезды мастеров
- PG: incremental-snapshot позволяет захватывать одну таблицу за другой - видимо чтобы не держать одну большую транзакцию
- PG: при изменении первичных ключей предлагается переводить бд в read-only и рестартовать дебезиум - чтобы все прошло корректно
- PG: 'Debezium can generate events that represent transaction boundaries and that enrich data change event messages'
- PG: оригинальный дебезиум поддерживает truncate event, мы - нет
- PG: оригинальный дебезиум поддерживает какие-то message events, но только с плагином pgoutput только для постгри 14+
- PG: в оригинальном дебезиуме "The wal2json plug-in is deprecated and scheduled for removal"
