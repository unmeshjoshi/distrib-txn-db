package kv;

import clock.HybridTimestamp;

import java.util.Map;
import java.util.HashMap;
import java.util.Optional;

public class Table {
    private final String tableId;
    private final MVCCStore store;

    public Table(String tableId, MVCCStore store) {
        this.tableId = tableId;
        this.store = store;
    }

    public boolean put(String rowKey, String columnId, HybridTimestamp timestamp, String value) {
        // Storage design note:
        // This table maps each column to its own logical MVCC key instead of serializing the whole
        // row into a single value. Per-column keys make partial updates, sparse rows, and as-of
        // row reconstruction natural. A whole-row value would be simpler and more compact for full
        // row reads, but every column update would rewrite the entire row and row history would be
        // tracked at row granularity instead of per column.
        String physicalKey = tableId + "_" + rowKey + "_" + columnId;
        return store.put(
                new MVCCKey(MemcomparableCodec.encodeString(physicalKey), timestamp),
                MemcomparableCodec.encodeString(value)
        );
    }

    public boolean insertRow(String rowKey, Map<String, String> columns, HybridTimestamp timestamp) {
        Map<MVCCKey, byte[]> mutations = new HashMap<>();
        for (Map.Entry<String, String> entry : columns.entrySet()) {
            String physicalKey = tableId + "_" + rowKey + "_" + entry.getKey();
            mutations.put(
                    new MVCCKey(MemcomparableCodec.encodeString(physicalKey), timestamp),
                    MemcomparableCodec.encodeString(entry.getValue())
            );
        }
        return store.putBatch(mutations);
    }

    public Optional<String> get(String rowKey, String columnId, HybridTimestamp readTimestamp) {
        String physicalKey = tableId + "_" + rowKey + "_" + columnId;
        return store.getAsOf(new MVCCKey(MemcomparableCodec.encodeString(physicalKey), readTimestamp))
                .map(MemcomparableCodec::decodeString);
    }

    public Optional<String> get(String rowKey, String columnId) {
        String physicalKey = tableId + "_" + rowKey + "_" + columnId;
        return store.getLatest(MemcomparableCodec.encodeString(physicalKey))
                .map(MemcomparableCodec::decodeString);
    }

    public Map<String, String> getRow(String rowKey, HybridTimestamp readTimestamp) {
        byte[] physicalPrefix = MemcomparableCodec.encodeString(tableId + "_" + rowKey + "_");
        Map<byte[], byte[]> fullMap = store.scanPrefixAsOf(physicalPrefix, readTimestamp);
        
        Map<String, String> columns = new HashMap<>();
        for (Map.Entry<byte[], byte[]> entry : fullMap.entrySet()) {
            String fullKeyStr = MemcomparableCodec.decodeString(entry.getKey());
            String colId = fullKeyStr.substring(MemcomparableCodec.decodeString(physicalPrefix).length()); // extract column identifier safely
            columns.put(colId, MemcomparableCodec.decodeString(entry.getValue()));
        }
        return columns;
    }

    public Map<String, String> getRow(String rowKey) {
        return getRow(rowKey, new HybridTimestamp(Long.MAX_VALUE, Integer.MAX_VALUE));
    }
}
