# distrib-txn-db

`distrib-txn-db` is a workshop-style Java project that builds up a distributed transactional key-value store step by step.

The `main` branch is the workshop/exercise branch. The `solutions` branch contains the complete reference implementation for all modules in this repository.

The project starts with hybrid logical clocks and MVCC, then adds distributed routing, transaction records, intents, clock uncertainty handling, and finally a simplified serializable layer.

The code is intentionally organized as small modules so each concept can be learned in isolation before moving to the next one.


## Modules

- `tickloom-hello-world`
  Minimal Tickloom request/response example. This is the smallest possible introduction to the process/message model used by the rest of the project.

- `01-hybrid-clock`
  Implements a Hybrid Logical Clock (`HybridClock`) and `HybridTimestamp`.

- `02-versioned-kv`
  Introduces MVCC key encoding, versioned reads/writes, prefix scans, and a simple table abstraction on top of the MVCC store.

- `03-distrib-versioned-kv`
  Adds distributed request routing with Tickloom. Keys are routed to storage nodes using hashing, and both clients and nodes propagate HLC timestamps on every request/response.

- `04-distrib-txn-kv`
  Implements distributed transactions with:
  - begin transaction
  - in-memory transaction records on the coordinator
  - provisional write intents
  - read-your-own-writes
  - commit and resolve
  - intent resolution through transaction status lookups
  - snapshot-isolation anomaly demonstrations

- `05-clock-uncertainty-and-read-restart`
  Builds on `04` and adds read restart for clock uncertainty. This module demonstrates how a snapshot read can miss a value inside the uncertainty window, and how restart fixes that.

- `06-serializable-txn`
  Builds on the earlier modules and adds a simplified serializable layer using read provisional records to prevent write skew.

## Project Themes

Across the modules, the project explores these ideas:

- Hybrid Logical Clocks
- MVCC key encoding and versioned storage
- Memcomparable / order-preserving key layouts
- Distributed routing by `hash(key)` and `hash(txnId)`
- Transaction coordinator records
- Write intents and intent resolution
- Snapshot isolation
- Clock uncertainty and read restart
- Serializable conflict prevention

## Snapshot Isolation Intuition

In this project, snapshot isolation can be read as:

- if a transaction is trying to write a key that some other transaction already committed after this
  transaction's snapshot/read timestamp, then it must abort
- otherwise, the transaction might be applying a decision based on stale data for that same key

The important gap is write skew:

- two transactions can read related data
- then write different keys
- so no write-write conflict is detected
- both may still commit under snapshot isolation

Read restart is the complementary piece that protects the snapshot itself from clock-skew
ambiguity:

- if a read may have missed a value because it falls inside the uncertainty window, the transaction
  restarts at a later snapshot timestamp
- after that restart, later write-write conflict checks can trust that the transaction is making
  decisions from a safe snapshot rather than from a skewed read

## Build And Test

Run the whole project:

```bash
./gradlew clean build
```

Run a single module:

```bash
./gradlew :04-distrib-txn-kv:test
```

Some useful module-level test commands:

```bash
./gradlew :01-hybrid-clock:test
./gradlew :02-versioned-kv:test
./gradlew :03-distrib-versioned-kv:test
./gradlew :04-distrib-txn-kv:test
./gradlew :05-clock-uncertainty-and-read-restart:test
./gradlew :06-serializable-txn:test
```

## Reading Order

The recommended order is:

1. `tickloom-hello-world`
2. `01-hybrid-clock`
3. `02-versioned-kv`
4. `03-distrib-versioned-kv`
5. `04-distrib-txn-kv`
6. `05-clock-uncertainty-and-read-restart`
7. `06-serializable-txn`

## Notes

- The code is optimized for teaching and exploration rather than production completeness.
- Some modules intentionally show anomalies first and then introduce the fix in a later module.

## Acknowledgments

OpenAI Codex was very helpful in scaffolding parts of the code and refining some of the workshop material.
