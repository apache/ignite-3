package org.apache.ignite.internal.schema.builder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.schema.SchemaTable;
import org.apache.ignite.schema.builder.HashIndexBuilder;
import org.apache.ignite.schema.builder.PartialIndexBuilder;

public class SchemaTableBuilderImpl implements org.apache.ignite.schema.builder.SchemaTableBuilder {

    public static org.apache.ignite.schema.builder.SchemaTableBuilder tableBuilder(String tableName) {
        return new SchemaTableBuilderImpl(DEFAULT_SCHEMA_NAME, tableName);
    }

    public static org.apache.ignite.schema.builder.SchemaTableBuilder tableBuilder(String schemaName, String tableName) {
        return new SchemaTableBuilderImpl(schemaName, tableName);
    }

    public static SchemaTableBuilderImpl tableBuilder(Class keyClass, Class valueClass) {
        // TODO: implement.

        return new SchemaTableBuilderImpl(DEFAULT_SCHEMA_NAME, valueClass.getSimpleName());
    }

    private final Map<String, TableColumnBuilderImpl> columns = new HashMap<>();
    private final Map<String, AbstractIndexBuilder> indices = new HashMap<>();
    private SortedIndexBuilderImpl pkIndex;
    private String[] affinityColumns;

    private final String tableName;
    private final String schemaName; //TODO: rename to 'namespace' ??

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

    @Override public org.apache.ignite.schema.builder.SchemaTableBuilder affinityColumns(String... affColumns) {
        this.affinityColumns = affColumns;

        return this;
    }

    @Override public SortedIndexBuilderImpl pk() {
        return new SortedIndexBuilderImpl(this).withName(PRIMARY_KEY_INDEX_NAME);
    }

    @Override public SortedIndexBuilderImpl addSortedIndex(String indexName) {
        assert !PRIMARY_KEY_INDEX_NAME.equals(indexName);

        return new SortedIndexBuilderImpl(this).withName(indexName);
    }

    @Override public PartialIndexBuilder addPartialIndex(String indexName) {
        assert !PRIMARY_KEY_INDEX_NAME.equals(indexName);

        return new PartialIndexBuilderImpl(this).withName(indexName);
    }

    @Override public HashIndexBuilder addHashIndex(String indexName) {
        assert !PRIMARY_KEY_INDEX_NAME.equals(indexName);

        return new HashIndexBuilderImpl(this).withName(indexName);
    }

    void addColumn(TableColumnBuilderImpl bld) {
        if (columns.put(bld.name(), bld) != null)
            throw new IllegalArgumentException("Column with same name already exists: " + bld.name());
    }

    public void addIndex(AbstractIndexBuilder bld) {
        if (PRIMARY_KEY_INDEX_NAME.equals(bld.name())) {
            assert bld.getClass() == SortedIndexBuilderImpl.class;

            pkIndex = (SortedIndexBuilderImpl)bld;
        }
        else if (indices.put(bld.name(), bld) != null)
            throw new IllegalArgumentException("Index with same name already exists: " + bld.name());
    }

    @Override public SchemaTable build() {
        assert schemaName != null : "Table name was not specified.";
        assert columns.size() >= 2 : "Key or/and value columns was not defined.";

        assert indices.values().stream()
            .filter(SortedIndexBuilderImpl.class::isInstance)
            .map(SortedIndexBuilderImpl.class::cast)
            .flatMap(idx -> idx.columns().stream())
            .map(SortedIndexBuilderImpl.SortedIndexColumnBuilderImpl::name).allMatch(columns::containsKey) : "Indexed column dosn't exists in schema.";

        assert pkIndex != null && !pkIndex.columns().isEmpty() : "PK index is not configured";

        final Set<String> keyCols = pkIndex.columns().stream().map(SortedIndexBuilderImpl.SortedIndexColumnBuilderImpl::name).collect(Collectors.toSet());

        assert keyCols.stream().allMatch(columns::containsKey) : "Undefined key column.";
        assert Arrays.stream(affinityColumns).allMatch(keyCols::contains) : "Affinity column must be a valid key column.";

        return null; // TODO: implement.
    }
}
