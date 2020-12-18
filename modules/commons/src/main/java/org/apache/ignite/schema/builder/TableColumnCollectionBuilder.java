package org.apache.ignite.schema.builder;

/**
 * Column collection builder.
 */
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
