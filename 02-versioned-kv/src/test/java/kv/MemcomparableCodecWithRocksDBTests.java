package kv;

import clock.HybridTimestamp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MemcomparableCodecWithRocksDBTests {

    private static byte[] newTimestamp(long wallClockTime) {
        HybridTimestamp t = new HybridTimestamp(wallClockTime, 0);
        return MemcomparableCodec.encode(t);
    }



    @Test
    public void rocksDBStoresKeysInAscendingOrder(@TempDir java.nio.file.Path tempDir) throws RocksDBException {
        RocksDB rocksDB = RocksDbMvccStore.createInstance(tempDir);
        rocksDB.put(newTimestamp(1000), "value1".getBytes());
        rocksDB.put(newTimestamp(2000), "value2".getBytes());
        rocksDB.put(newTimestamp(5000), "value5".getBytes());

        RocksIterator rocksIterator = rocksDB.newIterator();
        rocksIterator.seek(newTimestamp(Long.MAX_VALUE)); //goes to key <= key.// with Inverted keys.
        assertEquals("value5", new String(rocksIterator.value()));
        rocksIterator.next();
        assertEquals("value2", new String(rocksIterator.value()));
        rocksIterator.next();
        assertEquals("value1", new String(rocksIterator.value()));
        //The keys are sorted as 5000->2000->1000

        rocksIterator.seek(newTimestamp(3000)); //<=4 should give version 2
        assertEquals("value2", new String(rocksIterator.value()));
    }

    @Test
    public void rocksDBSeeksToPrefix(@TempDir java.nio.file.Path tempDir) throws RocksDBException {
        RocksDB rocksDB = RocksDbMvccStore.createInstance(tempDir);
        TestDB db = new TestDB(rocksDB);
        db.put("author", new HybridTimestamp(1000, 0), "Martin");
        db.put("author", new HybridTimestamp(2000, 0), "Unmesh");
        db.put("title", new HybridTimestamp(1500, 0), "The Art of Computer Programming");

        //for the same key prefix, keys are ordered inversely author_2000->author_1000

        RocksIterator rocksIterator = rocksDB.newIterator();
        rocksIterator.seek(MemcomparableCodec.encodeString("author"));
        assertEquals("Unmesh", new String(rocksIterator.value()));
        rocksIterator.next();
        assertEquals("Martin", new String(rocksIterator.value()));
        rocksIterator.next();
        assertEquals("The Art of Computer Programming", new String(rocksIterator.value()));
    }

    static class TestDB {
        private final RocksDB rocksDB;
        public TestDB(RocksDB rocksDB) throws RocksDBException {
            this.rocksDB = rocksDB;
        }
        public void put(String key, HybridTimestamp timestamp, String value) throws RocksDBException {
            rocksDB.put(MemcomparableCodec.encodeMVCCKey(new MVCCKey(key.getBytes(), timestamp)), MemcomparableCodec.encodeString(value));
        }
    }
}
