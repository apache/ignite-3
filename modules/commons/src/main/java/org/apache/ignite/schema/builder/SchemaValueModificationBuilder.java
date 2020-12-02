package org.apache.ignite.schema.builder;

public interface SchemaValueModificationBuilder extends SchemaColumnCollectionBuilder<SchemaValueColumnBuilder> {
    SchemaValueModificationBuilder dropColumn(String name);

    SchemaModificationBuilder done();
}
