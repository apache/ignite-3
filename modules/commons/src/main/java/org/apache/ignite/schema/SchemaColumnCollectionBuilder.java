package org.apache.ignite.schema;

public interface SchemaColumnCollectionBuilder<T> {
    T addColumn(String name);

    SchemaBuilder done();
}
