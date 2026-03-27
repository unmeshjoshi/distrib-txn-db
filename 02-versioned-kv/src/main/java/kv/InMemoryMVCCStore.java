package kv;

import clock.HybridTimestamp;

import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

public class InMemoryMVCCStore implements MVCCStore {
    private final ConcurrentSkipListMap<MVCCKey, byte[]> kvStore = new ConcurrentSkipListMap<>();

    @Override
    public boolean put(MVCCKey key, byte[] value) {
        kvStore.put(key, value);
        return true;
    }

    @Override
    public boolean delete(MVCCKey key) {
        kvStore.remove(key);
        return true;
    }

    @Override
    public boolean putBatch(Map<MVCCKey, byte[]> mutations) {
        for (Map.Entry<MVCCKey, byte[]> entry : mutations.entrySet()) {
            kvStore.put(entry.getKey(), entry.getValue());
        }
        return true;
    }

    @Override
    public Optional<byte[]> getAsOf(MVCCKey searchKey) {
        Map.Entry<MVCCKey, byte[]> entry = kvStore.floorEntry(searchKey);
        
        Optional<byte[]> result = Optional.empty();
        if (entry != null && Arrays.equals(entry.getKey().getKey(), searchKey.getKey())) {
            result = Optional.of(entry.getValue());
        }

        return result;
    }

    @Override
    public Optional<byte[]> getLatest(byte[] key) {
        return getAsOf(new MVCCKey(key, new HybridTimestamp(Long.MAX_VALUE, Integer.MAX_VALUE)));
    }

    @Override
    public Map<HybridTimestamp, byte[]> getVersionsUpTo(byte[] key, HybridTimestamp asOfTime) {
        ConcurrentNavigableMap<MVCCKey, byte[]> history = kvStore.subMap(
                new MVCCKey(key, new HybridTimestamp(Long.MIN_VALUE, Integer.MIN_VALUE)),
                true,
                new MVCCKey(key, asOfTime),
                true
        );
        Map<HybridTimestamp, byte[]> result = new LinkedHashMap<>();
        for (Map.Entry<MVCCKey, byte[]> entry : history.entrySet()) {
            if (Arrays.equals(entry.getKey().getKey(), key)) {
                result.put(entry.getKey().getTimestamp(), entry.getValue());
            }
        }
        return result;
    }

    @Override
    public Map<byte[], byte[]> scanPrefixAsOf(byte[] prefix, HybridTimestamp asOfTime) {
        ConcurrentNavigableMap<MVCCKey, byte[]> entries =
                kvStore.tailMap(new MVCCKey(prefix, new HybridTimestamp(Long.MIN_VALUE, Integer.MIN_VALUE)));
        Map<byte[], byte[]> result = new TreeMap<>(Arrays::compareUnsigned);
        for (Map.Entry<MVCCKey, byte[]> entry : entries.entrySet()) {
            byte[] logicalKey = entry.getKey().getKey();
            if (!startsWith(logicalKey, prefix)) {
                break;
            }
            if (entry.getKey().getTimestamp().compareTo(asOfTime) <= 0) {
                result.put(logicalKey, entry.getValue());
            }
        }
        return result;
    }

    @Override
    public void tick() {
    }

    @Override
    public void close() {
        kvStore.clear();
    }

    private boolean startsWith(byte[] value, byte[] prefix) {
        return value.length >= prefix.length
                && Arrays.compareUnsigned(value, 0, prefix.length, prefix, 0, prefix.length) == 0;
    }
}
