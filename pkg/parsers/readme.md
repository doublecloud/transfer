# Parsers

## Parser configs

Parser configuratios (`parser_config`) can be of two types: 

* **lb** (logbroker)

* **common** (kafka/yds/eventhub)

This is because historically the `lb-source` had one set of parsers (five of them), and the other source queues had only `json`.

And even the json parser, which was present in both **lb** and **common**, contained more parsing settings for `lb-source`.

So the parser could be in the `lb-source`, it could be in the other source queues, it could be in both, and the `lb-source` could contain one set of settings and the other source queues could contain a different set of settings.

For example:

* A `tskv-parser` is only present in `lb-source`

* A `json-parser` is present both in the `lb-source` and other source queues; in the `lb-source`, the json has more settings.

So we arrive at a scheme where the parser is a set of the following entities:

* A separate set of settings (`parser_config`) for each type of source: `lb/common`

* A single parser, which can be initialized by any `parser_config`.

## Naming conventions

Adding a new parser is done in such a way that you don't have to think about things like:

* How it will be stored in the source queue models

* How the dispatching works between proto-models and your `parser_config`.

This magic is implemented through reflection, and you'll have to use the following naming conventions when describing your parser:

* `parser_config` should be named according to the template:
    
    * for **common**: `parser_config_%PARSER_NAME%_common.go` - and should implement the `CommonParserConfig` interface
    
    * for **lb**: `parser_config_%PARSER_NAME%_lb.go` - and should implement the `LbParserConfig` interface

* The parser itself should be called `parser_%PARSER_NAME%.go` - and should implement the `AbstractParser` interface

There should be a constructor function for the parser, for example:

```go
func NewParserAuditTrailsV1(inWrapped interface{}, topicName string, sniff bool, logger log.Logger, registry *stats.SourceStats) (registrylib.AbstractParser, error) {
```

Such a constructor function takes any `parser_config` as its first parameter, and should be able to construct a parser from any of its `parser_config`s.

To register parser and its `parser_config`, you need to call the `registrylib.RegisterParser` function in `init()` in the parser file:

```go
func init() {
    registrylib.RegisterParser(
        NewParserAuditTrailsV1,
        []registrylib.AbstractParserConfig{new(ParserConfigAuditTrailsV1Common)},
    )
}
```

Where we pass:

* A parser constructor function

* A `parser_config` array


And for this to be connected to the assembly, you need to be hooked into `pkg/parsers/registry/registry.go`.

For example, you can check how parsers are declared: `tskv/json`

**Recommendation** - since the parser directory already contains many files and entities - if the parsing engine itself takes more than one file, put it in the `/engine` subdirectory.

## Step-by-step alogrith to add a new parser

* For each `parser_config` type (lb/common), you describe UI proto-models (`transfer_manager/go/proto/api/console/form/parsers.proto`) add them to `oneof` in `ParserConfigLb/ParserConfinCommon` (I suggest you forget about API proto-model until they make this api more usable)

* Create a subdirectory in `pkg/parsers/registry` - for example, `myparser`

* For each type of `parser_config` (**lb** / **common**) you create `parser_config` (and tests) - for example, `parser_config_myparser_lb.go`, `parser_config_myparser_common.go` (converters to proto API can be filled with `return nil, nil`)

* Create parser description (for example, `parser_myparser.go`) and `add init()` function with `registrylib.RegisterParser` call

* Add import to this parser in `pkg/parsers/registry/registry.go`

* Add it to `pkg/parsers/registrylib/canon/unit_test.go` - if your parser gets there, then it will get everywhere you need.
