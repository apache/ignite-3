package org.apache.ignite.schema;

/**
 * Table column descriptor.
 */
public interface Column {
    /**
     * @return Columnt type.
     */
    ColumnType type();

    /**
     * @return Column name.
     */
    String name();

    /**
     * Nullable flag.
     *
     * @return {@code True} if null-values is allowed, {@code false} otherwise.
     */
    boolean nullable();

    /**
     * @return Default column value.
     */
    Object defaultValue();

    /**
     * @return {@code true} if column belong to key, {@code false} otherwise.
     */
    boolean isKeyColumn();

    /**
     * @return {@code true} if column is affinity column, {@code false} otherwise.
     */
    boolean isAffinityColumn();
}
