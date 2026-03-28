# Exercise

This module is a guided walkthrough, not a coding exercise.

The goal is to understand how the local MVCC store from `02-versioned-kv` becomes a distributed
key-value store using Tickloom messaging.

Before this module, go through:

- [tickloom-hello-world](/Users/unmeshjoshi/work/distrib-txn-db/tickloom-hello-world)

Then use this module to trace:

- how the client picks the owning node for a key
- how a storage node handles read and write requests
- how Hybrid Logical Clock (HLC) timestamps flow between client and node

Suggested walkthrough:

1. Read [StorageReplicaClient.java](/Users/unmeshjoshi/work/distrib-txn-db/03-distrib-versioned-kv/src/main/java/com/distrib/versioned/kv/StorageReplicaClient.java)
2. Read [StorageReplica.java](/Users/unmeshjoshi/work/distrib-txn-db/03-distrib-versioned-kv/src/main/java/com/distrib/versioned/kv/StorageReplica.java)
3. Run:

```bash
GRADLE_USER_HOME=/Users/unmeshjoshi/work/distrib-txn-db/.gradle-home ./gradlew :03-distrib-versioned-kv:test
```

4. Study [StorageReplicaClusterTest.java](/Users/unmeshjoshi/work/distrib-txn-db/03-distrib-versioned-kv/src/test/java/com/distrib/versioned/kv/StorageReplicaClusterTest.java)

Focus on understanding the request flow and routing model. No code changes are required in this module.
