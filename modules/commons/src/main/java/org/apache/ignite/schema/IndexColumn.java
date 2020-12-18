package org.apache.ignite.schema;

/**
 * Index column descriptor.
 */
public interface IndexColumn {
    /**
     * @return Column name.
     */
    String name();

    /**
     * @return {@code True} for ascending sort order, {@code false} otherwise.
     */
    boolean asc();
}
