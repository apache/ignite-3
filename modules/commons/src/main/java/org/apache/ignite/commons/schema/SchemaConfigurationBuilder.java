package org.apache.ignite.commons.schema;

import java.util.HashMap;
import java.util.Map;

public class SchemaConfigurationBuilder {
    public static SchemaConfigurationBuilder create() {
        return new SchemaConfigurationBuilder();
    }

    public static SchemaConfigurationBuilder create(Class keyClass, Class valueClass) {
        // TODO: implement.

        return new SchemaConfigurationBuilder();
    }

    private final Map<String, ColumnConfigurationBuilder> columns = new HashMap<>();
    private final Map<String, IndexConfigurationBuilder> indices = new HashMap<>();
    private final Map<String, String> aliasMap = new HashMap<>();

    private String tableName;

    SchemaConfigurationBuilder withName(String tableName) {
        this.tableName = tableName;

        return this;
    }

    public String name() {
        return tableName;
    }

    public KeyColumnConfigurationBuilder addKeyColumn(String name) {
        return new KeyColumnConfigurationBuilder(this).withName(name).notNull();
    }

    public ColumnConfigurationBuilder addValueColumn(String name) {
        return new ColumnConfigurationBuilder(this).withName(name);
    }

    public IndexConfigurationBuilder addIndex(String name) {
        return new IndexConfigurationBuilder(this).withName(name);
    }

    void addColumn(ColumnConfigurationBuilder bld) {
        if (columns.put(bld.name(), bld) != null)
            throw new IllegalArgumentException("Column with same name already exists: " + bld.name());

    }

    public SchemaConfigurationBuilder addAlias(String alias, String columnName) {
        if (aliasMap.put(alias, columnName) != null)
            throw new IllegalArgumentException("Alias with same name already exists: " + alias);

        return this;
    }

    public void build() {
        assert tableName != null :"Table name was not specified.";
        assert !columns.isEmpty() : "No columns was defined.";
        assert columns.values().stream().anyMatch(c -> c instanceof KeyColumnConfigurationBuilder && !c.isNullable()) : "At least one non-null key column must exists.";
        assert columns.values().stream().allMatch(c ->
            !(c instanceof KeyColumnConfigurationBuilder)
                || !((KeyColumnConfigurationBuilder)c).isAffinityColumn()
                || !c.isNullable()) : "Nullable affinity column is not allowed.";

        assert indices.values().stream().flatMap(idx -> idx.columns().stream()).allMatch(idxCol -> columns.containsKey(idxCol.name())) : "Unknown index column.";

        assert aliasMap.keySet().stream().noneMatch(columns::containsKey) : "Alias hides existed column.";
        assert aliasMap.values().stream().allMatch(columns::containsKey) : "Alias for unknown column.";

    }

    public void addIndex(IndexConfigurationBuilder bld) {
        if (indices.put(bld.name(), bld) != null)
            throw new IllegalArgumentException("Index with same name already exists: " + bld.name());
    }
}
