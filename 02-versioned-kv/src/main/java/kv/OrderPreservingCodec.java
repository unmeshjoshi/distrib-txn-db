package kv;

import clock.HybridTimestamp;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Universal serialization boundary responsible for mapping logical application types
 * (String, Int, Long) into RocksDB's Memcomparable (Order-Preserving) byte arrays.
 * It also centrally manages the complex physical formatting of the MVCC versions.
 * <p>
 * Order-Preserving (Memcomparable) Encoding is a strict requirement for distributed
 * Wide-Column and Document databases utilizing lexicographical byte iterators (like RocksDB or WiredTiger).
 * Without mathematical bit-inversion on signed types (like executing `value ^ 0x80000000`), a negative integer
 * (-1 = 0xFF...) would incorrectly evaluate as strictly greater than a positive integer
 * (1 = 0x00...) natively during unsigned byte scans.
 * <p>
 * Real-world Distributed Engine Implementations:
 * - CockroachDB Standard Key Encoding: https://github.com/cockroachdb/cockroach/blob/master/pkg/util/encoding/encoding.go
 * - YugabyteDB DocDB Key Formatting: https://github.com/yugabyte/yugabyte-db/blob/master/src/yb/dockv/doc_key.cc#L251
 * - MongoDB (WiredTiger) Memcomparable KeyString Format: https://github.com/mongodb/mongo/blob/master/src/mongo/db/storage/key_string/key_string.cpp
 */
public class OrderPreservingCodec {

    // --- Basic Order-Preserving Encoders ---

    public static byte[] encodeString(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    public static String decodeString(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static byte[] encodeInt(int value) {
        // Toggle the sign bit to preserve byte-wise lexicographical ordering
        // This ensures -1 (0xFF..) evaluates as logically smaller than 1 (0x00..) underneath
        int toggled = value ^ 0x80000000;
        return ByteBuffer.allocate(Integer.BYTES).putInt(toggled).array();
    }

    public static int decodeInt(byte[] bytes) {
        int toggled = ByteBuffer.wrap(bytes).getInt();
        return toggled ^ 0x80000000;
    }

    public static byte[] encodeLong(long value) {
        long toggled = value ^ 0x8000000000000000L;
        return ByteBuffer.allocate(Long.BYTES).putLong(toggled).array();
    }

    public static long decodeLong(byte[] bytes) {
        long toggled = ByteBuffer.wrap(bytes).getLong();
        return toggled ^ 0x8000000000000000L;
    }

    // --- MVCC Physical Formatting ---

    /**
     * Encodes a logical MVCC representation into a physical RocksDB backend key.
     * Format: `[LogicalKeyBytes] + [0x00] + [InvertedWallClock] + [InvertedTicks]`
     */
    public static byte[] encodeMVCCKey(MVCCKey key) {
        byte[] keyBytes = key.getKey();
        // A separator byte (0x00) is architecturally MANDATORY for RocksDB range bounds!
        // It acts as a definitive boundary between the logical key and the timestamp suffix.
        // Because 0x00 is the lowest possible byte value, it ensures that a shorter logical
        // key strictly sorts before any longer key that shares the same prefix.
        //
        // WHAT FAILS IF SKIPPED:
        // If we omit the 0x00 separator, the timestamp bytes immediately follow the logical key.
        // Because we bit-invert timestamps (often resulting in high bytes like 0xFF),
        // the key "User" + [0xFF...] would lexicographically sort AFTER "User1" + [0x...].
        // All versions of "User" would no longer group contiguously, breaking prefix scans
        // and causing queries for "User" to bleed into or miss records for "User1".
        ByteBuffer buffer = ByteBuffer.allocate(keyBytes.length + 1 + Long.BYTES + Integer.BYTES);
        buffer.put(keyBytes);
        buffer.put((byte) 0x00); // Null terminal physical separator

        // Bit-Inversion: Assigns newer timestamps practically smaller byte sequences
        // so that iteration scans natively yield the highest timestamps rapidly.
        putHybridTimestamp(buffer, key.getTimestamp());
        return buffer.array();
    }

    /**
     * Reconstructs exclusively the logical prefix key dynamically by scanning upwards
     * evaluating natively away from the MVCC formatting byte blobs.
     */
    public static byte[] decodeMVCCLogicalKey(byte[] dbPhysicalKey) {
        return decodeMVCCKey(dbPhysicalKey).getKey();
    }

    public static MVCCKey decodeMVCCKey(byte[] dbPhysicalKey) {
        // Because timestamps are strictly 12 bytes (8 Long + 4 Int), the null separator
        // is guaranteed structurally to be positioned at exactly length - 13!
        int nullPos = getTimestampSeparatorPosition(dbPhysicalKey);
        byte[] logicalKey = java.util.Arrays.copyOf(dbPhysicalKey, nullPos);
        ByteBuffer buf = ByteBuffer.wrap(dbPhysicalKey, nullPos + 1, Long.BYTES + Integer.BYTES);
        long invertedTime = buf.getLong();
        int invertedTicks = buf.getInt();
        return new MVCCKey(logicalKey, new HybridTimestamp(~invertedTime, ~invertedTicks));
    }

    /**
     * Retrieves the absolute chronological constraints dynamically mapped directly downwards
     * stripping away logical string overlays from the parsed map scan index structures.
     */
    public static HybridTimestamp decodeMVCCTimestamp(byte[] dbPhysicalKey) {
        return decodeMVCCKey(dbPhysicalKey).getTimestamp();
    }

    public static byte[] encode(HybridTimestamp t) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        putHybridTimestamp(buffer, t);
        return buffer.array();
    }

    private static void putHybridTimestamp(ByteBuffer buffer, HybridTimestamp timestamp) {
        buffer.putLong(~timestamp.getWallClockTime());
        buffer.putInt(~timestamp.getTicks());
    }

    private static int getTimestampSeparatorPosition(byte[] dbPhysicalKey) {
        int nullPos = dbPhysicalKey.length - 13;
        if (nullPos < 0 || dbPhysicalKey[nullPos] != 0x00) {
            throw new IllegalArgumentException("Corrupted DB MVCC Physical Key Structure");
        }
        return nullPos;
    }
}
