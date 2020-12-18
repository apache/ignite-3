package org.apache.ignite.schema;

/**
 * Node local mapper defines mapping rules for current available key/value class version.
 *
 * Note: When you change your class, you may want to upgrade the mapper as well, and vice versa.
 *
 * Note: Mapper is configured on per-node basis and allows to have different versions of the same class
 * on different nodes. This makes possible smooth schema upgrade with changing user key-value classes via
 * rolling restart.
 *
 * Note: Data and schema consistency is fully determined with schema mode and user actions order,
 * and have nothing to do with the mapper.
 */
public interface ColumnMapper {
    /**
     * Gets column name mapped to field name.
     *
     * @param columnName Field name.
     * @return Field name.
     */
    String columnToField(String columnName);
}
