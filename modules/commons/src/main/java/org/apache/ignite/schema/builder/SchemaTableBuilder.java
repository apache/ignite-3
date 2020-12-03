package org.apache.ignite.schema.builder;

import org.apache.ignite.schema.SchemaTable;

public interface SchemaTableBuilder {
    /** Default schema name. */
    public static final String DEFAULT_SCHEMA_NAME = "PUBLIC";

    /** Primary key index name. */
    public static final String PRIMARY_KEY_INDEX_NAME = "PK";

    /**
     * @return Tabke columns builder.
     */
    TableColumnCollectionBuilder columns();

    /**
     * @param colNames Column names.
     * @return {@code this} for chaining.
     */
    SchemaTableBuilder affinityColumns(String... colNames); // TODO: is it a part of PK ?

    /**
     * @return Primary key builder.
     */
    SortedIndexBuilder pk();

    /**
     * @param indexName Index name.
     * @return Sorted index builder.
     */
    SortedIndexBuilder addSortedIndex(String indexName);

    /**
     * @param indexName Index name.
     * @return Partial index builder.
     */
    PartialIndexBuilder addPartialIndex(String indexName);

    /**
     * @param indexName Index name.
     * @return Hash index builder.
     */
    HashIndexBuilder addHashIndex(String indexName);

    /**
     * Build table schema.
     *
     * @return Table schema.
     */
    SchemaTable build();
}
