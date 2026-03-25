package clock;

import com.tickloom.util.Clock;

/**
 * A Hybrid Logical Clock (HLC) that generates HybridTimestamps.
 *
 * Hybrid Logical Clocks combine a physical wall-clock time with a logical counter. This ensures
 * timestamps remain close to physical time while providing strictly monotonic ordering of events 
 * across a distributed system, essential for multi-version concurrency control (MVCC).
 * 
 * References to real-world distributed database implementations:
 * - CockroachDB: https://github.com/cockroachdb/cockroach/tree/master/pkg/util/hlc
 * - YugabyteDB: https://github.com/yugabyte/yugabyte-db/blob/master/src/yb/common/hybrid_time.h
 * - MongoDB: https://github.com/mongodb/mongo/blob/master/src/mongo/db/logical_clock.h
 */
public class HybridClock {
    /**
     * Note on Clock implementation:
     * We use TickLoom's `com.tickloom.util.Clock` rather than JDK 21's `java.time.Clock`.
     * While `java.time.Clock` is inherently immutable, TickLoom provides a testkit with 
     * mutable stubs (e.g., `StubClock`) that allow us to manually advance time. This is 
     * highly suitable for deterministic simulation testing within distributed systems.
     */
    private final Clock clock;
    private HybridTimestamp latestTime;

    public HybridClock() {
        this(new com.tickloom.util.SystemClock());
    }

    public HybridClock(Clock clock) {
        this.clock = clock;
        this.latestTime = new HybridTimestamp(clock.now(), 0);
    }

    public HybridTimestamp now() {
        long currentTimeMillis = clock.now();
        if (latestTime.getWallClockTime() >= currentTimeMillis) {
            latestTime = new HybridTimestamp(latestTime.getWallClockTime(), latestTime.getTicks() + 1);
        } else {
            latestTime = new HybridTimestamp(currentTimeMillis, 0);
        }
        return latestTime;
    }

    public HybridTimestamp tick(HybridTimestamp requestTime) {
        long currentTimeMillis = clock.now();
        long maxTime = Math.max(currentTimeMillis, Math.max(latestTime.getWallClockTime(), requestTime.getWallClockTime()));

        int ticks = 0;
        if (maxTime == latestTime.getWallClockTime() && maxTime == requestTime.getWallClockTime()) {
            ticks = Math.max(latestTime.getTicks(), requestTime.getTicks()) + 1;
        } else if (maxTime == latestTime.getWallClockTime()) {
            ticks = latestTime.getTicks() + 1;
        } else if (maxTime == requestTime.getWallClockTime()) {
            ticks = requestTime.getTicks() + 1;
        }

        latestTime = new HybridTimestamp(maxTime, ticks);
        return latestTime;
    }
}
