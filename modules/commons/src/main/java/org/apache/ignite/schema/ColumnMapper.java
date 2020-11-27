package org.apache.ignite.schema;

/**
 * Node specific mapper.
 */
public interface ColumnMapper {
    /**
     * Gets column name mapped to field name.
     *
     * @param fieldName Field name.
     * @return Column name.
     */
    String columnToField(String fieldName);
}
