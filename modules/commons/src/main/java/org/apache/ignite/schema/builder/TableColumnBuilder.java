package org.apache.ignite.schema.builder;

import org.apache.ignite.schema.Column;
import org.apache.ignite.schema.ColumnType;

/**
 * Table column builder.
 */
public interface TableColumnBuilder {
    /**
     * Sets column type.
     *
     * @param columnType Column type.
     * @return {@code this} for chaining.
     */
    TableColumnBuilder withType(ColumnType columnType);

    /**
     * Mark column as nullable.
     *
     * @return {@code this} for chaining.
     */
    TableColumnBuilder asNullable();

    /**
     * Mark column as non-nullable.
     *
     * @return {@code this} for chaining.
     */
    TableColumnBuilder asNonNull();

    /**
     * Sets column default value.
     *
     * @param defValue Default value.
     * @return {@code this} for chaining.
     */
    TableColumnBuilder withDefaultValue(Object defValue);

    /**
     * Builds column.
     *
     * @return Built column.
     */
    Column build();
}

