## Exactly Once Support

### **1. Sequence Diagram**

A sequence diagram illustrate the interaction between components (`deduper`, `store`, `sink`, and `batch`) over time. It focuses on the order of operations.

Key Steps to Include:
- `deduper` interacts with `store` to retrieve the last block.
- `deduper` evaluates the current `batch` and decides actions (skip, retry, insert).
- `deduper` pushes valid items to sink.
- `State` transitions (`before` → `after`) occur in `store`.

```mermaid
sequenceDiagram
    participant deduper
    participant store
    participant sink
    participant batch

    deduper->>store: Get(lastBlock, lastStatus)
    alt LastBlock is nil
        deduper->>store: Set(currBlock, Before)
        deduper->>sink: Push(batch)
        deduper->>store: Set(currBlock, After)
    else LastBlock exists
        deduper->>batch: Split(batch into skip, retry, insert)
        alt Retry Needed
            deduper->>sink: Push(retry)
            deduper->>store: Set(lastBlock, After)
        end
        alt Insert Needed
            deduper->>store: Set(currBlock, Before)
            deduper->>sink: Push(insert)
            deduper->>store: Set(currBlock, After)
        end
    end
```

---

### **2. Activity Diagram**
An activity diagram focuses on the flow of decisions and actions in the `deduper.Process` method. It maps the logic's flow to identify branches and repetitive operations.

Here's the corrected version of the **Activity Diagram** in Mermaid syntax:

```mermaid
flowchart TD
    Start --> CheckLastBlock
    CheckLastBlock -->|No Last Block| SetBefore
    CheckLastBlock -->|Last Block Exists| SplitBatch

    SetBefore --> PushBatch
    PushBatch --> SetAfter
    SetAfter --> End

    SplitBatch -->|Skip| SkipItems
    SplitBatch -->|Retry| RetryBlock
    SplitBatch -->|Insert| InsertBlock

    RetryBlock --> RetryAfter
    RetryAfter --> SetAfter

    InsertBlock --> SetBefore2
    SetBefore2 --> PushBatch2
    PushBatch2 --> SetAfter2
    SetAfter2 --> End

    %% Labels for steps
    CheckLastBlock[Check Last Block]
    SetBefore[Set Block to Before]
    PushBatch[Push Batch to Sink]
    SetAfter[Set Block to After]
    SplitBatch[Split Batch]
    SkipItems[Log Skipped Items]
    RetryBlock[Retry Last Block]
    RetryAfter[Set Last Block to After]
    InsertBlock[Insert New Block]
    SetBefore2[Set New Block to Before]
    PushBatch2[Push Insert Batch to Sink]
    SetAfter2[Set New Block to After]
    End[End]
```

---

### **3. Component Diagram**
**Class Diagram**: diagram shows the relationships and dependencies between the key components in the system (e.g., `deduper`, `sink`, `store`, and `batch`).

```mermaid
classDiagram
    class Deduper {
        - Sinker sink
        - InsertBlockStore store
        - TablePartID part
        + Process(batch []ChangeItem) func() error
        - splitBatch(batch []ChangeItem, lastBlock *InsertBlock) (skip, retry, insert)
    }

    class InsertBlockStore {
        + Get(part TablePartID) (block *InsertBlock, status InsertBlockStatus, err error)
        + Set(part TablePartID, block *InsertBlock, status InsertBlockStatus) error
    }

    class Sinker {
        + Push(batch []ChangeItem) error
    }

    class Batch {
        - Offset() (uint64, bool)
    }

    Deduper --> InsertBlockStore : uses
    Deduper --> Sinker : uses
    Deduper --> Batch : processes
```

**Component Diagram**: highlights the `ExactlyOnceSink` as a new component and shows how it interacts with the existing components (`sink`, `store`, and `deduper`).


```mermaid
graph TD
    subgraph "Exactly Once"
        direction LR
        A --> D[deduper]
        A[ExactlyOnceSink] -->|Uses| C[InsertBlockStore]
        D[deduper] -->|Split| F[InsertBlock]
    end


    D[deduper] -->|Push| G[Non-Exactly Once Sink abstract.Sinker]

    %% Labels
    A[ExactlyOnceSink]
    C[InsertBlockStore]
    D[deduper]
    F[InsertBlock]

    %% Style
    classDef highlighted fill:#f9f,stroke:#333,stroke-width:2px;
```

---

### **4. Data Flow Diagram**
A data flow diagram shows how data (e.g., blocks and batches) moves through the system.

```mermaid
flowchart TD
    Input[Input: Batch of Items] --> Deduper[Deduper Logic]
    Deduper -->|New Block| StoredBefore[Store: Before]
    Deduper -->|Skip Items| Log[Log Skipped Items]
    Deduper -->|Retry Block| Sink[Sink: Push Batch]
    StoredBefore -->|Insert Block| Sink
    Sink --> StoreAfter[Store: After]
```

---

### **5. Decision Tree Diagram**
A decision tree diagram is useful for showing the branching logic in `splitBatch`.

```mermaid
graph TD
    Start[Start: Iterate Batch] --> OffsetCheck[Check Item Offset]
    OffsetCheck -->|Offset < lastBlock.min| Skip[Add Item to Skip]
    OffsetCheck -->|Offset >= lastBlock.min AND Offset < lastBlock.max| Retry[Add Item to Retry]
    OffsetCheck -->|Offset >= lastBlock.max| Insert[Add Item to Insert]
    Skip --> Next[Next Item]
    Retry --> Next
    Insert --> Next
    Next -->|More Items| OffsetCheck
    Next -->|No More Items| End[Split Complete]
```
