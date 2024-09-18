# `sequences`: canonized sequences of ChangeItems in replication

This directory contains code to **canonize** sequences of ChangeItems in replication for usage in sink correctness tests. This is not an actual test.

## Usage

The output of canonization is stored in the [**sub**directories of `canondata`](./canondata/). Each `extracted` file in those directories contains a canonized sequence of ChangeItems obtained by execution of a `SNAPSHOT_AND_INCREMENT` transfer with `PostgreSQL` source and `Mock` target. It is possible to set a particular SQL for initialization (which is not transformed into canonized items) and for replication parts of each test. SQLs are in the [`dump` subdirectory](./dump/).

### Add a new canonized sequence

In order to add a new canonized sequence, do the following:

1. Add a SQL file(s) for initialization (optional) and for replication (required) in the [`dump` subdirectory](./dump/).
2. Import the new file(s) in the [`sequences_test.go` file](./sequences_test.go) using the `//go:embed` directive, as is done with other files. Add the `GO_TEST_EMBED_PATTERN` to the [`ya.make` file](./ya.make).
3. Add a `t.Run()` call and a corresponding test name to the list of tests/
4. Recanonize the sequences using `ya make -AZ .` run from the current directory.
