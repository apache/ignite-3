package org.apache.ignite.internal.schema.builder;

import org.apache.ignite.internal.schema.ColumnImpl;
import org.apache.ignite.schema.Column;
import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.builder.TableColumnBuilder;

public class TableColumnBuilderImpl implements TableColumnBuilder {

    private String colName;
    private ColumnType columnType;
    private boolean nullable;
    private Object defValue;

    public TableColumnBuilderImpl(String colName) {
        this.colName = colName;
    }

    @Override public TableColumnBuilderImpl withType(ColumnType columnType) {
        this.columnType = columnType;

        return this;
    }

    @Override public TableColumnBuilderImpl asNullable() {
        nullable = true;

        return this;
    }

    @Override public TableColumnBuilderImpl asNonNull() {
        nullable = false;

        return this;
    }

    @Override public TableColumnBuilderImpl withDefaultValue(Object defValue) {
        this.defValue = defValue;

        return this;
    }

    String name() {
        return colName;
    }

    boolean isNullable() {
        return nullable;
    }

    @Override public Column build() {
        return new ColumnImpl(colName);
    }
}
