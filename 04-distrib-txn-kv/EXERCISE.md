# Exercise

This is the core hands-on module of the workshop.

In this module, participants build a distributed transactional key-value store step by step on top
of:

- Hybrid Logical Clocks from `01-hybrid-clock`
- MVCC storage from `02-versioned-kv`
- distributed request routing from `03-distrib-versioned-kv`

The workflow for this module is:

- run a focused test
- implement the missing transaction behavior in [TransactionalStorageReplica.java](/Users/unmeshjoshi/work/distrib-txn-db/04-distrib-txn-kv/src/main/java/com/distrib/txn/kv/TransactionalStorageReplica.java)
- re-run the test until it passes

## Exercise 1: Begin Transaction

Implement:

- `BeginTransaction` handling
- creation of a `TxnRecord` with status `PENDING`

Run:

```bash
GRADLE_USER_HOME=/Users/unmeshjoshi/work/distrib-txn-db/.gradle-home ./gradlew :04-distrib-txn-kv:test --tests com.distrib.txn.kv.TransactionalStorageReplicaCoreFlowTest.beginTransactionCreatesPendingTxnRecordOnCoordinator
```

What to learn:

- the coordinator is chosen by `hash(txnId)`
- the transaction record must exist before any writes happen

Code marker:

- `//TODO: Exercise 1. Create a Transaction record.`

## Exercise 2: Provisional Write Intents

Implement:

- transactional write handling
- writing a provisional record to the `intentStore`

Run:

```bash
GRADLE_USER_HOME=/Users/unmeshjoshi/work/distrib-txn-db/.gradle-home ./gradlew :04-distrib-txn-kv:test --tests com.distrib.txn.kv.TransactionalStorageReplicaCoreFlowTest.txnWriteStoresIntentAndReadReturnsOwnIntent
```

What to learn:

- writes are provisional first
- committed data and provisional intents are stored separately

Code marker:

- `//Exercise 2. Implement this.`

## Exercise 3: Transactional Reads

Implement:

- read-your-own-writes from the intent store
- otherwise read committed data at the transaction's `readTimestamp`

Run:

```bash
GRADLE_USER_HOME=/Users/unmeshjoshi/work/distrib-txn-db/.gradle-home ./gradlew :04-distrib-txn-kv:test --tests com.distrib.txn.kv.TransactionalStorageReplicaCoreFlowTest.txnWriteStoresIntentAndReadReturnsOwnIntent --tests com.distrib.txn.kv.TransactionalStorageReplicaCoreFlowTest.txnReadCommittedValuesAtReadTimestamp
```

What to learn:

- a transaction reads from one fixed snapshot
- a transaction must still see its own writes

Code area:

- `beginRead(...)`

## Exercise 4: Commit And Resolve

Implement:

- marking a transaction `COMMITTED`
- sending resolve requests to participants
- moving intents into the committed store
- deleting provisional records

Run:

```bash
GRADLE_USER_HOME=/Users/unmeshjoshi/work/distrib-txn-db/.gradle-home ./gradlew :04-distrib-txn-kv:test --tests com.distrib.txn.kv.TransactionalStorageReplicaCoreFlowTest.commitMovesIntentToCommittedStoreAndMarksTransactionCommitted
```

What to learn:

- commit is coordinated by the transaction coordinator
- cleanup happens on participant nodes

Code marker:

- `//Exercise 4. Implement commitTransaction.`

## Follow-On: Resolve Intents From Other Transactions

Implement:

- when a read or write sees an intent from another transaction:
  - ask that transaction's coordinator for status
  - if `PENDING`, treat it as a conflict or ignore it depending on the operation
  - if `COMMITTED`, resolve it and continue
  - if `ABORTED`, delete it and continue

Run:

```bash
GRADLE_USER_HOME=/Users/unmeshjoshi/work/distrib-txn-db/.gradle-home ./gradlew :04-distrib-txn-kv:test --tests com.distrib.txn.kv.TransactionalStorageReplicaWriteResolutionTest --tests com.distrib.txn.kv.TransactionalStorageReplicaReadResolutionTest
```

What to learn:

- why transaction records are created early
- how lazy resolution works
- why reads and writes can repair old unresolved intents

## Follow-On: Snapshot Isolation Anomalies

This final part is a discussion/demo step rather than a coding step.

Use the anomaly tests to understand what snapshot isolation still does not prevent:

- lost update
- write skew
- clock uncertainty on snapshot reads

Run:

```bash
GRADLE_USER_HOME=/Users/unmeshjoshi/work/distrib-txn-db/.gradle-home ./gradlew :04-distrib-txn-kv:test --tests com.distrib.txn.kv.LostUpdateAnomalyTest --tests com.distrib.txn.kv.SnapshotIsolationAnomalyTest --tests com.distrib.txn.kv.ClockUncertaintySnapshotTest
```

These limitations motivate the next modules:

- `05-clock-uncertainty-and-read-restart`
- `06-serializable-txn`

## Suggested Implementation Order

1. Exercise 1: begin transaction
2. Exercise 2: provisional write
3. Exercise 3: transactional read
4. Exercise 4: commit and resolve
5. Follow-on: intent resolution for reads and writes
6. Follow-on: study the anomaly tests
