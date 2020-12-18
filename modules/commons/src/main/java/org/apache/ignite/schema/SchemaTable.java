package org.apache.ignite.schema;

import java.util.Collection;
import java.util.List;
import org.apache.ignite.schema.modification.TableModificationBuilder;

/**
 * Schema table descriptor.
 */
public interface SchemaTable {
    /**
     * @return Key columns.
     */
    List<Column> keyColumns();

    /**
     * @return Value columns.
     */
    List<Column> valueColumns();

    /**
     * @return Table name.
     */
    String tableName();

    /**
     * Schema name + Table name
     *
     * @return Canonical table name.
     */
    String canonicalName();

    /**
     * @return Table indexes.
     */
    Collection<TableIndex> indices();

    /**
     * @return Schema modification builder.
     */
    TableModificationBuilder toBuilder();
}
