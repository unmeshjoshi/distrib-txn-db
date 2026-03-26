package kv;

import clock.HybridTimestamp;

import java.util.HashMap;
import java.util.Map;
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
        // Caveat: this key format is still ambiguous because it relies on "_" as a delimiter.
        // A row key / column id pair such as ("a_b", "c") collides with ("a", "b_c").
        // A future refactor should move this into a structured key codec with explicit boundaries.
        // YugabyteDB's DocKey encoding is a good reference for this kind of structured key layout:
        // https://github.com/yugabyte/yugabyte-db/blob/master/src/yb/dockv/doc_key.cc
        String physicalKey = tableId + "_" + rowKey + "_" + columnId;
        return store.put(
                new MVCCKey(OrderPreservingCodec.encodeString(physicalKey), timestamp),
                OrderPreservingCodec.encodeString(value)
        );
    }

    public boolean insertRow(Row row, HybridTimestamp timestamp) {
        Map<MVCCKey, byte[]> mutations = new HashMap<>();
        for (Column column : row.getColumns()) {
            String physicalKey = tableId + "_" + row.getKey() + "_" + column.name();
            mutations.put(
                    new MVCCKey(OrderPreservingCodec.encodeString(physicalKey), timestamp),
                    OrderPreservingCodec.encodeString(column.value())
            );
        }
        return store.putBatch(mutations);
    }

    public Optional<String> get(String rowKey, String columnId, HybridTimestamp readTimestamp) {
        String physicalKey = tableId + "_" + rowKey + "_" + columnId;
        return store.getAsOf(new MVCCKey(OrderPreservingCodec.encodeString(physicalKey), readTimestamp))
                .map(OrderPreservingCodec::decodeString);
    }

    public Optional<String> get(String rowKey, String columnId) {
        String physicalKey = tableId + "_" + rowKey + "_" + columnId;
        return store.getLatest(OrderPreservingCodec.encodeString(physicalKey))
                .map(OrderPreservingCodec::decodeString);
    }

    public Row getRow(String rowKey, HybridTimestamp readTimestamp) {
        byte[] physicalPrefix = OrderPreservingCodec.encodeString(tableId + "_" + rowKey + "_");
        Map<byte[], byte[]> fullMap = store.scanPrefixAsOf(physicalPrefix, readTimestamp);
        Row row = new Row(rowKey);
        String decodedPrefix = OrderPreservingCodec.decodeString(physicalPrefix);
        for (Map.Entry<byte[], byte[]> entry : fullMap.entrySet()) {
            String fullKeyStr = OrderPreservingCodec.decodeString(entry.getKey());
            // This substring logic depends on the same delimiter-based encoding caveat described
            // above. It is correct only as long as table/row/column identifiers do not create
            // ambiguous composite keys.
            String colId = fullKeyStr.substring(decodedPrefix.length());
            row.addColumn(colId, OrderPreservingCodec.decodeString(entry.getValue()));
        }
        return row;
    }

    public Row getRow(String rowKey) {
        return getRow(rowKey, new HybridTimestamp(Long.MAX_VALUE, Integer.MAX_VALUE));
    }
}
