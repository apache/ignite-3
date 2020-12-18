package org.apache.ignite.schema.modification;

import org.apache.ignite.schema.TableIndex;

/*
 * Collect schema modification commands and pass them to manager to create schema upgrade script.
 *
 * Upgrade script should implements commands in next order regardless the user call order:
 * dropAlias, dropIndex
 * dropColumn
 * addColumn
 * addAlias, addIndex
 */
public interface SchemaModificationBuilder {
    SchemaTableModificationBuilder alterTable(String tableName);

    SchemaModificationBuilder addIndex(TableIndex index);

    SchemaModificationBuilder dropIndex(String name);

    void build();
}
