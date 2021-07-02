package org.apache.ignite.internal.schema.mapping;

/**
 * Column mapper builder interface.
 */
public interface ColumnaMapperBuilder {
    /**
     * Add column mapping.
     *
     * @param from Source column index.
     * @param to Target column index.
     */
    public void add(int from, int to);

    /**
     * @return Column mapper.
     */
    ColumnMapper build();
}
