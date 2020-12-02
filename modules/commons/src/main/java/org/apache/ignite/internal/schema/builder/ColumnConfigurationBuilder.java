package org.apache.ignite.internal.schema.builder;

import org.apache.ignite.schema.ColumnType;
import org.apache.ignite.schema.builder.SchemaColumnBuilder;

public abstract class ColumnConfigurationBuilder<T extends SchemaColumnBuilder<T>> implements SchemaColumnBuilder<T> {
    private String colName;
    private ColumnType columnType;
    private boolean nullable;
    private Object defValue;

    @Override public T withType(ColumnType columnType) {
        this.columnType = columnType;

        return getThis();
    }

    @Override public T nullable() {
        nullable = true;

        return getThis();
    }

    @Override public T notNull() {
        nullable = false;

        return getThis();
    }

    @Override public T withName(String colName) {
        this.colName = colName;

        return getThis();
    }

    @Override public T defaultValue(Object defValue) {
        this.defValue = defValue;

        return getThis();
    }

    String name() {
        return colName;
    }

    boolean isNullable() {
        return nullable;
    }

    protected abstract T getThis();
}
