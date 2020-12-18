package org.apache.ignite.schema.builder;

import org.apache.ignite.schema.SchemaTable;
import org.apache.ignite.schema.TableIndex;

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
     * @return Primary key builder.
     */
    PrimaryKeyBuilder pk();

    /**
     * @param index Table index.
     * @return Sorted index builder.
     */
    SchemaTableBuilder withindex(TableIndex index);

    /**
     * Build table schema.
     *
     * @return Table schema.
     */
    SchemaTable build();
}
