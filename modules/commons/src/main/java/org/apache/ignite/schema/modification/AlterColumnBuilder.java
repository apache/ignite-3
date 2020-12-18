package org.apache.ignite.schema.modification;

import org.apache.ignite.schema.ColumnType;

/**
 * Alter column builder.
 *
 * NOTE: Only safe actions that can be applied automatically on-fly are allowed.
 */
public interface AlterColumnBuilder {
    /**
     * Renames column.
     *
     * @param newName New column name.
     * @return {@code this} for chaining.
     */
    AlterColumnBuilder withNewName(String newName);

    /**
     * Convert column to a new type.
     * <p>
     * Note: New type must be compatible with old.
     *
     * @param newType New column type.
     * @return {@code this} for chaining.
     */
    AlterColumnBuilder convertTo(ColumnType newType);

    /**
     * Sets new column default value.
     *
     * @param defaultValue Default value.
     * @return {@code this} for chaining.
     */
    AlterColumnBuilder withNewDefault(Object defaultValue);

    /**
     * Mark column as nullable.
     *
     * @return {@code this} for chaining.
     */
    AlterColumnBuilder asNullable();

    /**
     * Mark column as non-nullable.
     *
     * Note: Replacement param is mandatory, all previously stored 'nulls'
     * will be treated as replacement value on read.
     *
     * @param replacement Non null value, that 'null' will be converted to.
     * @return {@code this} for chaining.
     */
    AlterColumnBuilder asNonNullable(Object replacement);

    /**
     * @return Parent builder.
     */
    TableModificationBuilder done();
}
