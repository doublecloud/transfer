# Hand-canonized data for Debezium unit tests

This folder contains canonized data for different Debezium unit tests.

It is possible that recanonization of original items is required, because the output of a source in Data Transfer can change over time. In this case, recanonization will be required.

## Recanonize

### `emitter_chain_test__canon_change_item_original.txt`

`emitter_chain_test__canon_change_item_original.txt` is extracted from [`tests/e2e/pg2pg/debezium/all_datatypes_serde/test-results/all_datatypes_serde/testing_out_stuff/go.out`](../../../../../tests/e2e/pg2pg/debezium/all_datatypes_serde/test-results/all_datatypes_serde/testing_out_stuff/go.out). This output file can be obtained by running [`tests/e2e/pg2pg/debezium/all_datatypes_serde`](../../../../../tests/e2e/pg2pg/debezium/all_datatypes_serde) test.

After running the test, look through the output file to find the lines starting with `changeItem dump:`. There will be multiple such lines; the one with the desired item contains an item of kind `insert` for table `public.basic_types`, so it is likely to contain the substring `"kind":"insert","schema":"public","table":"basic_types"`. The first match in the output file is the one we are looking for.

In the item found, replace the `nextlsn` and `commitTime` values with the ones from the previous version of the canonized item in `emitter_chain_test__canon_change_item_original.txt`.
