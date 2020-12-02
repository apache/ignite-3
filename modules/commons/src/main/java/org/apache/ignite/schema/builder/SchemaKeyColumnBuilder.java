package org.apache.ignite.schema.builder;

public interface SchemaKeyColumnBuilder extends SchemaColumnBuilder<SchemaKeyColumnBuilder> {
    SchemaKeyColumnBuilder affinityColumn();

    SchemaKeyBuilder done();
}
