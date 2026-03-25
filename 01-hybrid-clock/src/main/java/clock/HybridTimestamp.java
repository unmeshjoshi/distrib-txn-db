package clock;

import java.util.Objects;

public class HybridTimestamp implements Comparable<HybridTimestamp> {
    private final long wallClockTime;
    private final int ticks;

    public HybridTimestamp(long wallClockTime, int ticks) {
        this.wallClockTime = wallClockTime;
        this.ticks = ticks;
    }

    public long getWallClockTime() {
        return wallClockTime;
    }

    public int getTicks() {
        return ticks;
    }

    @Override
    public int compareTo(HybridTimestamp other) {
        if (this.wallClockTime == other.wallClockTime) {
            return Integer.compare(this.ticks, other.ticks);
        }
        return Long.compare(this.wallClockTime, other.wallClockTime);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HybridTimestamp that = (HybridTimestamp) o;
        return wallClockTime == that.wallClockTime && ticks == that.ticks;
    }

    @Override
    public int hashCode() {
        return Objects.hash(wallClockTime, ticks);
    }

    @Override
    public String toString() {
        return "HybridTimestamp{" +
                "wallClockTime=" + wallClockTime +
                ", ticks=" + ticks +
                '}';
    }
}
