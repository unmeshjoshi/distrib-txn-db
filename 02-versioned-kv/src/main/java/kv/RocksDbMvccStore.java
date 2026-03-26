package kv;

import clock.HybridTimestamp;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static kv.OrderPreservingCodec.*;

public final class RocksDbMvccStore implements MVCCStore, AutoCloseable {
    static {
        RocksDB.loadLibrary();
    }

    private final RocksDB db;

    public RocksDbMvccStore(Path dataDirectory) {
        try {
            this.db = createInstance(dataDirectory);
        } catch (RocksDBException e) {
            throw new IllegalStateException("Unable to open RocksDB store", e);
        }
    }

    public static RocksDB createInstance(Path dataDirectory) throws RocksDBException {
        Options options = new Options().setCreateIfMissing(true);
        return RocksDB.open(options, dataDirectory.toAbsolutePath().toString());
    }

    private void putEncoded(byte[] key, byte[] value) {
        try {
            db.put(key, value);
        } catch (RocksDBException e) {
            throw new IllegalStateException("Unable to put key-value pair", e);
        }
    }

    @Override
    public boolean put(MVCCKey key, byte[] value) {
        putEncoded(encodeMVCCKey(key), value);
        return true;
    }

    @Override
    public boolean putBatch(Map<MVCCKey, byte[]> mutations) {
        try (WriteBatch writeBatch = new WriteBatch();
             WriteOptions writeOptions = new WriteOptions()) {
            for (Map.Entry<MVCCKey, byte[]> entry : mutations.entrySet()) {
                writeBatch.put(encodeMVCCKey(entry.getKey()), entry.getValue());
            }
            db.write(writeOptions, writeBatch);
            return true;
        } catch (RocksDBException e) {
            throw new IllegalStateException("Unable to write batch of key-value pairs", e);
        }
    }

    @Override
    public Optional<byte[]> getAsOf(MVCCKey searchKey) {
        byte[] encodedSearchKey = encodeMVCCKey(searchKey);
        try (RocksIterator iterator = db.newIterator()) {
            iterator.seek(encodedSearchKey);
            if (!iterator.isValid() || !belongsToLogicalKey(searchKey.getKey(), iterator.key())) {
                return Optional.empty();
            }
            return Optional.of(iterator.value());
        }
    }

    @Override
    public Optional<byte[]> getLatest(byte[] key) {
        try (RocksIterator iterator = db.newIterator()) {
            iterator.seek(key);
            if (!iterator.isValid()) {
                return Optional.empty();
            }
            if (belongsToKey(key, iterator.key()) || belongsToLogicalKey(key, iterator.key())) {
                return Optional.of(iterator.value());
            }
            return Optional.empty();
        }
    }

    @Override
    public Map<HybridTimestamp, byte[]> getVersionsUpTo(byte[] key, HybridTimestamp asOfTime) {
        Map<HybridTimestamp, byte[]> result = new LinkedHashMap<>();
        byte[] encodedSearchKey = encodeMVCCKey(new MVCCKey(key, asOfTime));
        try (RocksIterator iterator = db.newIterator()) {
            iterator.seek(encodedSearchKey);
            while (iterator.isValid() && belongsToLogicalKey(key, iterator.key())) {
                MVCCKey mvccKey = decodeMVCCKey(iterator.key());
                result.put(mvccKey.getTimestamp(), iterator.value());
                iterator.next();
            }
        }
        return result;
    }

    @Override
    public Map<byte[], byte[]> scanPrefixAsOf(byte[] prefix, HybridTimestamp asOfTime) {
        var result = new TreeMap<byte[], byte[]>(Arrays::compareUnsigned);
        try (RocksIterator iterator = db.newIterator()) {
            iterator.seek(prefix); //This seeks to the first version which is the latest.
            collectVisiblePrefixRecordsAsOf(prefix, asOfTime, iterator, result);
        }
        return result;
    }

    private void collectVisiblePrefixRecordsAsOf(
            byte[] prefix, HybridTimestamp asOfTime, RocksIterator iterator, Map<byte[], byte[]> result) {

        while (iterator.isValid()) {
            MVCCKey mvccKey = decodeMVCCKey(iterator.key());

            if (!mvccKey.startsWith(prefix)) {
                break;
            }

            byte[] logicalKey = mvccKey.getKey();
            if (!result.containsKey(logicalKey) && mvccKey.isVisibleAt(asOfTime)) {
                result.put(logicalKey, iterator.value());
            }

            iterator.next();
        }
    }

    @Override
    public void tick() {

    }

    private boolean belongsToKey(byte[] key, byte[] seekedKey) {
        return Arrays.equals(key, seekedKey);
    }

    private boolean belongsToLogicalKey(byte[] logicalKey, byte[] encodedKey) {
        return isEncodedMvccKey(encodedKey)
                && encodedKey.length > logicalKey.length
                && encodedKey[logicalKey.length] == 0x00
                && Arrays.equals(logicalKey, Arrays.copyOf(encodedKey, logicalKey.length));
    }

    private boolean isEncodedMvccKey(byte[] encodedKey) {
        return encodedKey.length >= Long.BYTES + Integer.BYTES + 1
                && encodedKey[encodedKey.length - (Long.BYTES + Integer.BYTES + 1)] == 0x00;
    }

    @Override
    public void close() {
        db.close();
    }
}
