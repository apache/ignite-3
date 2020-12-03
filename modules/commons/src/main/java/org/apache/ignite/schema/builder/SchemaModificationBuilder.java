package org.apache.ignite.schema.builder;

public interface SchemaModificationBuilder {
    Void addColumn();
    Void dropColumn();
    Void alterColumn();

    SortedIndexBuilder addIndex(String name);
    SortedIndexBuilder dropIndex(String name);

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
