package kv;

import clock.HybridTimestamp;

public final class TestUtils {
    private TestUtils() {
    }

    public static HybridTimestamp ts(long wallClockTime) {
        return new HybridTimestamp(wallClockTime, 0);
    }

    public static HybridTimestamp ts(long wallClockTime, int logical) {
        return new HybridTimestamp(wallClockTime, logical);
    }
}
