package org.apache.ignite.internal.schema;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.schema.SchemaBuilder;

public class SchemaConfigurationBuilder implements SchemaBuilder {
    public static SchemaBuilder create() {
        return new SchemaConfigurationBuilder();
    }

    public static SchemaConfigurationBuilder create(Class keyClass, Class valueClass) {
        // TODO: implement.

        return new SchemaConfigurationBuilder();
    }

    private final Map<String, ColumnConfigurationBuilder<?>> columns = new HashMap<>();
    private final Map<String, IndexConfigurationBuilder> indices = new HashMap<>();
    private final Map<String, String> aliasMap = new HashMap<>();

    private String tableName;

    @Override public SchemaConfigurationBuilder withName(String tableName) {
        this.tableName = tableName;

        return this;
    }

    String name() {
        return tableName;
    }

    @Override public KeyColumnCollectionBuilder keyColumns() {
        return new KeyColumnCollectionBuilder(this);
    }

    @Override public ValueColumnCollectionBuilder valueColumns() {
        return new ValueColumnCollectionBuilder(this);
    }

    @Override public IndexConfigurationBuilder addIndex(String name) {
        return new IndexConfigurationBuilder(this).withName(name);
    }

    void addColumn(ColumnConfigurationBuilder<?> bld) {
        if (columns.put(bld.name(), bld) != null)
            throw new IllegalArgumentException("Column with same name already exists: " + bld.name());
    }

    @Override public SchemaConfigurationBuilder addAlias(String alias, String columnName) {
        if (aliasMap.put(alias, columnName) != null)
            throw new IllegalArgumentException("Alias with same name already exists: " + alias);

        return this;
    }

    @Override public void build() {
        assert tableName != null : "Table name was not specified.";
        assert columns.size() >= 2 : "Key or/and value columns was not defined.";

        assert indices.values().stream().flatMap(idx -> idx.columns().stream())
            .map(IndexColumnConfigurationBuilder::name).allMatch(columns::containsKey) : "Indexed column dosn't exists in schema.";

        assert aliasMap.keySet().stream().noneMatch(columns::containsKey) : "Alias hides existed column.";
        assert aliasMap.values().stream().allMatch(columns::containsKey) : "Alias for unknown column.";
    }

    public void addIndex(IndexConfigurationBuilder bld) {
        if (indices.put(bld.name(), bld) != null)
            throw new IllegalArgumentException("Index with same name already exists: " + bld.name());
    }
}
