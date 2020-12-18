package org.apache.ignite.schema.builder;

import org.apache.ignite.schema.Column;
import org.apache.ignite.schema.SchemaTable;
import org.apache.ignite.schema.TableIndex;

/**
 * Table descriptor builder.
 */
public interface SchemaTableBuilder {
    /** Primary key index name. */
    public static final String PRIMARY_KEY_INDEX_NAME = "PK";

    /**
     * @return Table columns builder.
     */
    SchemaTableBuilder columns(Column... columns);

    /**
     * @return Primary key builder.
     */
    PrimaryKeyBuilder pk();

    /**
     * Adds index.
     *
     * @param index Table index.
     * @return Schema table builder for chaining.
     */
    SchemaTableBuilder withindex(TableIndex index);

    /**
     * Builds table schema.
     *
     * @return Table schema.
     */
    SchemaTable build();
}
