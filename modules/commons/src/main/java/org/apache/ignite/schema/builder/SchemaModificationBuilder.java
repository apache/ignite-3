package org.apache.ignite.schema.builder;

public interface SchemaModificationBuilder {
//    SchemaKeyBuilder keyColumns(); // Is this forbidden?
    SchemaValueBuilder valueColumns();

    SchemaIndexBuilder addIndex(String name);
    SchemaIndexBuilder dropIndex(String name);

    SchemaBuilder addAlias(String alias, String columnName);
    SchemaBuilder dropAlias(String alias);

    /*
     * Collect schema modification commands and pass them to manager to create schema upgrade script.
     *
     * Upgrade script should implements commands in next order regardless the user call order:
     * dropAlias, dropIndex
     * dropColumn
     * addColumn
     * addAlias, addIndex
     */
    void build();
}
