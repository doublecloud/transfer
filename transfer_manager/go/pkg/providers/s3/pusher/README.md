# The Pusher Interface

The pusher interface allows us to implement different pushing behavior depending on the transfer mechanism.
The interface defines two methods Push and Ack, both of these work with a chunk of data currently being processed.

### The Chunk
Chunk holds all necessary infos we need during processing.
- The slice of ChangeItems to push to target.
- The name fo the file these CI are coming from.
- Information if the current chunk is the last chunk of data from a file.
- The offset of the data we read (used in state tracking).
- The size of the processed data, used for throttling the read speed to not run OOM.

### Push
Push forwards a chunk of data to the underlying pusher, may this be sync or async pusher.

### Ack
Removes already processed chunks of data from state to keep the state clean. (In the case of async pusher)

### Snapshotting
In the case of a snapshotting transfer we use the default synchronous abstract.Pusher.
No real state management is necessary for the sync pusher since each batch of files is processed form start to finish before moving on to the next.

### Replication
For replication a parsqueue is used for async pushing. The pusher needs to keep a state of files being processed since the reader will keep reading new file
even though previous ones might not have been fully pushed to target.

Peculiarities of the Parsqueue pusher:

1. State of data chunks is tracked in memory so that we know if we are done processing a file from start to finish.
2. Since push's to the underlying queue happen asynchronously and are buffered in the parsequeue we need to throttle push speed to not run OOM.
3. State can be kept as small as possible since already done files are persisted either to DB state (for polling replication) or messages are deleted (for SQS)

