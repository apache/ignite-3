package org.apache.ignite.schema.builder;

import org.apache.ignite.schema.ColumnType;

public interface TableColumnBuilder {
    TableColumnBuilder withType(ColumnType columnType);

    TableColumnBuilder nullable();

    TableColumnBuilder notNull();

    TableColumnBuilder withName(String colName);

    TableColumnBuilder defaultValue(Object defValue);

    TableColumnCollectionBuilder done();
}

