package org.apache.ignite.schema.builder;

public interface SchemaValueModificationBuilder {

    TableColumnBuilder addColumn(String name);

    TableColumnBuilder alterColumn(String name);

    SchemaValueModificationBuilder dropColumn(String name);

    SchemaModificationBuilder done();
}
