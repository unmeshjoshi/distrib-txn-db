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

    //TODO: Implement this method.
    public HybridTimestamp now() {
      return tick(latestTime);
    }

    public HybridTimestamp tick(HybridTimestamp requestTime) {
        long currentWallClockTime = clock.now();
        long mergedWallClockTime = mergedWallClockTime(currentWallClockTime, requestTime);
        int mergedTicks = mergedTicks(mergedWallClockTime, requestTime);

        latestTime = new HybridTimestamp(mergedWallClockTime, mergedTicks);
        return latestTime;
    }

    private long mergedWallClockTime(long currentWallClockTime, HybridTimestamp requestTime) {
        return Math.max(
                currentWallClockTime,
                Math.max(latestTime.getWallClockTime(), requestTime.getWallClockTime())
        );
    }

    /**
     * Chooses the next logical tick once the merged wall-clock time is known.
     *
     * Example:
     * - latestTime  = (1000, 2)
     * - requestTime = (1000, 5)
     * - merged time = 1000
     *
     * Since both timestamps share the winning wall-clock time, the larger logical tick wins and
     * we return 6.
     *
     * If physical time wins, the logical tick resets to 0. For example:
     * - latestTime  = (1000, 2)
     * - requestTime = (999, 9)
     * - currentWallClockTime = clock.now() = 1005
     * - merged time = 1005
     *
     * In that case neither the local nor request timestamp supplied the winning wall-clock time,
     * so we start a new logical sequence at 0.
     */
    private int mergedTicks(long mergedWallClockTime, HybridTimestamp requestTime) {
        long latestWallClockTime = latestTime.getWallClockTime();
        long requestWallClockTime = requestTime.getWallClockTime();

        if (mergedWallClockTime == latestWallClockTime && mergedWallClockTime == requestWallClockTime) {
            return Math.max(latestTime.getTicks(), requestTime.getTicks()) + 1;
        }
        if (mergedWallClockTime == latestWallClockTime) {
            return latestTime.getTicks() + 1;
        }
        if (mergedWallClockTime == requestWallClockTime) {
            return requestTime.getTicks() + 1;
        }
        return 0;
    }

}
