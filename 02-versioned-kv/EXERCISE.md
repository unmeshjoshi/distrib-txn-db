# Exercise

Implement `compareTo(...)` in [MVCCKey.java](/Users/unmeshjoshi/work/distrib-txn-db/02-versioned-kv/src/main/java/kv/MVCCKey.java).

`MVCCKey` represents:

- a logical key
- a version timestamp for that key

The comparison needs to support MVCC ordering so that versioned values for the same logical key can
be stored and queried correctly.

Use the tests in [MVCCKeyTest.java](/Users/unmeshjoshi/work/distrib-txn-db/02-versioned-kv/src/test/java/kv/MVCCKeyTest.java) to drive the implementation.

Suggested workflow:

1. Run:

```bash
GRADLE_USER_HOME=/Users/unmeshjoshi/work/distrib-txn-db/.gradle-home ./gradlew :02-versioned-kv:test --tests kv.MVCCKeyTest
```

2. Implement `compareTo(...)`
3. Re-run the test until it passes

Focus only on `MVCCKey.compareTo(...)` for this exercise. The rest of the module can stay as-is for now.
