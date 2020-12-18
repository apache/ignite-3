package org.apache.ignite.internal.schema.builder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.schema.HashIndex;
import org.apache.ignite.schema.PartialIndex;
import org.apache.ignite.schema.SchemaTable;
import org.apache.ignite.schema.TableIndex;
import org.apache.ignite.schema.builder.PrimaryKeyBuilder;
import org.apache.ignite.schema.builder.SchemaTableBuilder;

public class SchemaTableBuilderImpl implements SchemaTableBuilder {

    public static org.apache.ignite.schema.builder.SchemaTableBuilder tableBuilder(String tableName) {
        return new SchemaTableBuilderImpl(DEFAULT_SCHEMA_NAME, tableName);
    }

    public static org.apache.ignite.schema.builder.SchemaTableBuilder tableBuilder(String schemaName,
        String tableName) {
        return new SchemaTableBuilderImpl(schemaName, tableName);
    }

    public static SchemaTableBuilderImpl tableBuilder(Class keyClass, Class valueClass) {
        // TODO: implement schema generation from classes.

        return new SchemaTableBuilderImpl(DEFAULT_SCHEMA_NAME, valueClass.getSimpleName());
    }

    private final Map<String, TableColumnBuilderImpl> columns = new HashMap<>();
    private final Map<String, TableIndex> indices = new HashMap<>();
    private PrimaryKeyBuilderImpl pkIndex;

    private final String tableName;
    private final String schemaName;

    SchemaTableBuilderImpl(String schemaName, String tableName) {
        this.schemaName = DEFAULT_SCHEMA_NAME;
        this.tableName = tableName;
    }

    String canonicalName() {
        return schemaName;
    }

    String tableName() {
        return tableName;
    }

    @Override public ColumnCollectionBuilder columns() {
        return new ColumnCollectionBuilder(this);
    }

    @Override public PrimaryKeyBuilder pk() {
        return new PrimaryKeyBuilderImpl(this);
    }

    @Override public SchemaTableBuilder indices(TableIndex index) {
        if (PRIMARY_KEY_INDEX_NAME.equals(index.name()))
            throw new IllegalArgumentException("Not valid index name for secondary index: " + index.name());
        else if (indices.put(index.name(), index) != null)
            throw new IllegalArgumentException("Index with same name already exists: " + index.name());

        return this;
    }

    @Override public SchemaTable build() {
        assert schemaName != null : "Table name was not specified.";
        assert columns.size() >= 2 : "Key or/and value columns was not defined.";

        validatePrimaryKey();

        validateSecondaryIndices();

        return null; // TODO: implement.
    }

    void addColumn(TableColumnBuilderImpl bld) {
        if (columns.put(bld.name(), bld) != null)
            throw new IllegalArgumentException("Column with same name already exists: " + bld.name());
    }

    private void validatePrimaryKey() {
        assert pkIndex != null : "PK index is not configured";
        assert pkIndex.columns().length > 0 && pkIndex.affinityColumns().length > 0 : "Primary key must have one affinity column at least";

        final Set<String> keyCols = Arrays.stream(pkIndex.columns()).collect(Collectors.toSet());

        assert keyCols.stream().allMatch(columns::containsKey) : "Key column must be a valid table column.";
        assert Arrays.stream(pkIndex.affinityColumns()).allMatch(keyCols::contains) : "Affinity column must be a valid key column.";
    }

    private void validateSecondaryIndices() {
        assert indices.values().stream()
            .filter(SortedIndexBuilderImpl.class::isInstance)
            .map(SortedIndexBuilderImpl.class::cast)
            .flatMap(idx -> idx.columns().stream())
            .map(SortedIndexBuilderImpl.SortedIndexColumnBuilderImpl::name)
            .allMatch(columns::containsKey) : "Indexed column dosn't exists in schema.";

        assert indices.values().stream()
            .filter(HashIndex.class::isInstance)
            .map(HashIndex.class::cast)
            .flatMap(idx -> idx.columns().stream())
            .allMatch(columns::containsKey) : "Indexed column dosn't exists in schema.";

        assert indices.values().stream()
            .filter(PartialIndex.class::isInstance)
            .map(PartialIndex.class::cast)
            .flatMap(idx -> idx.columns().stream())
            .allMatch(columns::containsKey) : "Indexed column dosn't exists in schema.";
    }
}
