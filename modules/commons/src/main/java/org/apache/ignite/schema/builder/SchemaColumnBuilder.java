package org.apache.ignite.schema.builder;

import org.apache.ignite.schema.ColumnType;

public interface SchemaColumnBuilder<T extends SchemaColumnBuilder<T>> {
    T withType(ColumnType columnType);

    T nullable();

    T notNull();

    T withName(String colName);

    T defaultValue(Object defValue);
}

