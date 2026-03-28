# Exercise

Implement `now()` in [HybridClock.java](/Users/unmeshjoshi/work/distrib-txn-db/01-hybrid-clock/src/main/java/clock/HybridClock.java).

The goal is to make the `HybridClockTest` tests pass by applying the hybrid logical clock rule for local time:

- if physical time has not advanced, keep the same wall-clock time and increment the logical tick
- if physical time has advanced, move to the new wall-clock time and reset the logical tick to `0`

Suggested workflow:

1. Run:

```bash
GRADLE_USER_HOME=/Users/unmeshjoshi/work/distrib-txn-db/.gradle-home ./gradlew :01-hybrid-clock:test
```

2. Implement `now()`
3. Re-run the tests until they pass

Focus only on `now()` for this exercise. `tick(...)` can stay as-is for now.
