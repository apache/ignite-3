package org.apache.ignite.schema.builder;

import org.apache.ignite.schema.ColumnType;

public interface TableColumnBuilder {
    TableColumnBuilder withType(ColumnType columnType);

    TableColumnBuilder asNullable();

    TableColumnBuilder asNotNull();

    TableColumnBuilder withName(String colName);

    TableColumnBuilder withDefaultValue(Object defValue);

    TableColumnCollectionBuilder done();
}

