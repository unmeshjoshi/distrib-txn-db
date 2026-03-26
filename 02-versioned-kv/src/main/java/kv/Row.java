package kv;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public final class Row {
    private final String key;
    private final Map<String, Column> columns = new LinkedHashMap<>();

    public Row(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public void addColumn(String name, String value) {
        columns.put(name, new Column(name, value));
    }

    public void addColumn(Column column) {
        columns.put(column.name(), column);
    }

    public Optional<Column> getColumn(String name) {
        return Optional.ofNullable(columns.get(name));
    }

    public String get(String name) {
        Column column = columns.get(name);
        return column == null ? null : column.value();
    }

    public int size() {
        return columns.size();
    }

    public boolean isEmpty() {
        return columns.isEmpty();
    }

    public Collection<Column> getColumns() {
        return Collections.unmodifiableCollection(columns.values());
    }
}
