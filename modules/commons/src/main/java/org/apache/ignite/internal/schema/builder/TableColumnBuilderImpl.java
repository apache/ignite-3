package org.apache.ignite.internal.schema.builder;

import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.builder.TableColumnBuilder;

public class TableColumnBuilderImpl implements TableColumnBuilder {
    private final ColumnCollectionBuilder columnCollectionBuilder;

    private String colName;
    private ColumnType columnType;
    private boolean nullable;
    private Object defValue;

    TableColumnBuilderImpl(ColumnCollectionBuilder parent) {
        this.columnCollectionBuilder = parent;
    }

    @Override public TableColumnBuilderImpl withType(ColumnType columnType) {
        this.columnType = columnType;

        return this;
    }

    @Override public TableColumnBuilderImpl nullable() {
        nullable = true;

        return this;
    }

    @Override public TableColumnBuilderImpl notNull() {
        nullable = false;

        return this;
    }

    @Override public TableColumnBuilderImpl withName(String colName) {
        this.colName = colName;

        return this;
    }

    @Override public TableColumnBuilderImpl defaultValue(Object defValue) {
        this.defValue = defValue;

        return this;
    }

    String name() {
        return colName;
    }

    boolean isNullable() {
        return nullable;
    }

    @Override public ColumnCollectionBuilder done() {
        columnCollectionBuilder.addColumn(this);

        return columnCollectionBuilder;
    }
}
