# YT Static Sink
## Transactions

    MainTx
    |
    |-- SubTx -> Init
    |
    |-- PartTx
    |   |
    |   |-- TableWriter -> Write
    |   |
    |   |-- TableWriter -> Write
    |   |
    |
    |-- SubTx -> Commit

MainTx - main transaction combines all sink actions during a snapshot.

PartTx - part transaction is a child of main transaction and combines all write operations of one part.

SubTx - sub transaction is a child of main or part transaction combines all actions of Init and Commit operations.

## State
Yt Static Sink stores the ID of the upper-level transaction in the transfer state (key-value state storage), which combines the transactions of all operations within this transfer.

This transaction is created before the push of the first InitShardedTableLoad item in beginMainTx(). Sending service items is sequential, so working with a transaction does not support parallel creation.

After the transaction has been created and saved to the state, all created sinks receive this state. Workers can read this state in parallel, because the transaction creates once in beginMainTx() on primary worker and does not change later.


## The stages of filling into a static table
To successfully complete the transfer of each table, the following functions must be performed. Each function performs in a sub-transactions and can be retried.

### Init
Creating a new table with the suffix _tmp. This function is called once for each table.

### Write
Each call to this function writes one chunk to the end of the static table. The writes can be executed in parallel.

### Commit
Commit function performs the final operations on a data-filled table. This function is called once for each table. Depending on the transfer settings and table schemas, the following actions are performed:

1. **Sort**

    If there are primary keys in the schema of the transferred table, create a temporary table with the _sorted suffix and run the sorting operation for the filled table, sending the result to the temporary _sorted table.

2. **Merge**

    If the cleaning policy is != Drop, create a temporary table with the _merged suffix and run the merge operation, sending its result to the temporary _merged table. Merge is launched with the maintenance of the sorting order and the option of chunking.

3. **Move**

    Moving the temporary table obtained in the previous stages to the user table.
