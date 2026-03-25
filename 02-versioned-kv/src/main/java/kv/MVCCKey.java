package kv;

import clock.HybridTimestamp;

import java.util.Arrays;

/**
 * Represents a logical key and its corresponding multi-version concurrency control (MVCC) timestamp.
 */
public class MVCCKey implements Comparable<MVCCKey> {
    private final byte[] key;
    private final HybridTimestamp timestamp;

    public MVCCKey(byte[] key, HybridTimestamp timestamp) {
        this.key = key;
        this.timestamp = timestamp;
    }

    public byte[] getKey() {
        return key;
    }

    public HybridTimestamp getTimestamp() {
        return timestamp;
    }

    @Override
    public int compareTo(MVCCKey other) {
        int keyCompare = Arrays.compareUnsigned(this.key, other.key);
        if (keyCompare != 0) {
            return keyCompare;
        }
        return this.timestamp.compareTo(other.timestamp);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MVCCKey that = (MVCCKey) o;
        return Arrays.equals(key, that.key) && timestamp.equals(that.timestamp);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + timestamp.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "MVCCKey{key=" + MemcomparableCodec.decodeString(key) +
                ", timestamp=" + timestamp + '}';
    }
}
