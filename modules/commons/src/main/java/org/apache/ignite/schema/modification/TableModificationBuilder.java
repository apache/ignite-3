package org.apache.ignite.schema.modification;

import org.apache.ignite.schema.Column;
import org.apache.ignite.schema.TableIndex;

/**
 * Collect schema modification commands and pass them to manager to create schema upgrade script.
 */
public interface TableModificationBuilder {
    /**
     * Adds new value column.
     *
     * @param column Column.
     * @return {@code this} for chaining.
     */
    TableModificationBuilder addColumn(Column column);

    /**
     * Adds new non-affinity key column.
     *
     * @param column Column.
     * @return {@code this} for chaining.
     */
    TableModificationBuilder addKeyColumn(Column column);

    /**
     * Creates alter column builder..
     *
     * @param columnName Column name.
     * @return Alter column builder.
     */
    AlterColumnBuilder alterColumn(String columnName);

    /**
     * Drops value column.
     * <p>
     * Note: Key column drop is not allowed.
     *
     * @param columnName Column.
     * @return {@code this} for chaining.
     */
    TableModificationBuilder dropColumn(String columnName);

    /**
     * Adds new table index.
     *
     * @param index Table index..
     * @return {@code this} for chaining.
     */
    TableModificationBuilder addIndex(TableIndex index);

    /**
     * Drops table index.
     * <p>
     * Note: PK can't be dropped.
     *
     * @param indexName Index name.
     * @return {@code this} for chaining.
     */
    TableModificationBuilder dropIndex(String indexName);

    /**
     * Applies changes.
     */
    void apply();
}
