### Canon for parsers

For each *TEST_CASE* we will create a single message parse with followed config:
```golang
	msg := persqueue.ReadMessage{
		Offset:      123,
		SeqNo:       32,
		SourceID:    []byte("test_source_id"),
		CreateTime:  time.Now(),
		WriteTime:   time.Now(),
		IP:          "192.168.1.1",
		Data:        `TEST_CASE.data`,
		ExtraFields: map[string]string{"some_field": "test"},
	}
```

#### Static definition

Each parser sample defined in 2 files:

```shell
{TEST_CASE}.config.json
{TEST_CASE}.data
```


#### Dynamic definition

Register your TestCase in `LoadDynamicTestCases` function in `canon_dynamic_test.go` function.
Handful in case when you need dynamically specify message while test runs (e.g. proto message tests).
