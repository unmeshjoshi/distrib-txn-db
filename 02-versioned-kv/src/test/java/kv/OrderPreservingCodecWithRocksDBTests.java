package kv;

import clock.HybridTimestamp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import static kv.TestUtils.ts;
import static org.junit.jupiter.api.Assertions.assertEquals;

class OrderPreservingCodecWithRocksDBTests {

    private static byte[] newTimestamp(long wallClockTime) {
        return OrderPreservingCodec.encode(ts(wallClockTime));
    }



    @Test
    public void invertedTimestampEncodingOrdersNewerVersionsFirstInRocksDB(
            @TempDir java.nio.file.Path tempDir) throws RocksDBException {
        RocksDB rocksDB = RocksDbMvccStore.createInstance(tempDir);
        rocksDB.put(newTimestamp(1000), "value1".getBytes());
        rocksDB.put(newTimestamp(2000), "value2".getBytes());
        rocksDB.put(newTimestamp(5000), "value5".getBytes());

        RocksIterator rocksIterator = rocksDB.newIterator();
        // RocksDB seek finds the first key >= target. Because timestamps are inverted,
        // seeking to the maximum logical timestamp lands on the newest stored version.
        rocksIterator.seek(newTimestamp(Long.MAX_VALUE));
        assertEquals("value5", new String(rocksIterator.value()));
        rocksIterator.next();
        assertEquals("value2", new String(rocksIterator.value()));
        rocksIterator.next();
        assertEquals("value1", new String(rocksIterator.value()));
        // The keys are sorted as 5000 -> 2000 -> 1000.

        // Seeking to an as-of timestamp returns the first version visible at or below that time.
        rocksIterator.seek(newTimestamp(3000));
        assertEquals("value2", new String(rocksIterator.value()));
    }

    @Test
    public void seekingToLogicalKeyPrefixReturnsNewestVersionFirst(
            @TempDir java.nio.file.Path tempDir) throws RocksDBException {
        RocksDB rocksDB = RocksDbMvccStore.createInstance(tempDir);
        putVersion(rocksDB, "author", ts(1000), "Martin");
        putVersion(rocksDB, "author", ts(2000), "Unmesh");
        putVersion(rocksDB, "title", ts(1500), "The Art of Computer Programming");

        //for the same key prefix, keys are ordered inversely author_2000->author_1000

        RocksIterator rocksIterator = rocksDB.newIterator();
        rocksIterator.seek(OrderPreservingCodec.encodeString("author"));
        assertEquals("Unmesh", new String(rocksIterator.value()));
        rocksIterator.next();
        assertEquals("Martin", new String(rocksIterator.value()));
        rocksIterator.next();
        assertEquals("The Art of Computer Programming", new String(rocksIterator.value()));
    }

    private static void putVersion(RocksDB rocksDB, String key, HybridTimestamp timestamp, String value)
            throws RocksDBException {
        rocksDB.put(
                OrderPreservingCodec.encodeMVCCKey(new MVCCKey(key.getBytes(), timestamp)),
                OrderPreservingCodec.encodeString(value));
    }
}
