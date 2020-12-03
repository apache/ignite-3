package org.apache.ignite.schema.builder;

public interface TableColumnCollectionBuilder {
    /**
     * @param columnName Column name.
     *
     * @return Column builder.
     */
    TableColumnBuilder addColumn(String columnName);

    /**
     * @return Parent builder.
     */
    SchemaTableBuilder done();
}
