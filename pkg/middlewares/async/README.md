# `middlewares/async`

The `middlewares/async` package contains middlewares to be used by source to provide asynchronous writes into the sink, enabling the sources to read concurrently.

Sinks (and their interface `abstract.Sink`) must stay as simple as possible. **When write concurrency** (parallel writes) **is desired, one should create multiple sinks** (with complete pipelines) instead of using the same sink for concurrent writes, as the latter enormously and unnecessarily complicates the implementation of a sink.

**To provide concurrency on the source side** and support both replication and snapshot sources, asynchonous middlewares in this package are provided.
