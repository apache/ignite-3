package org.apache.ignite.schema;

public interface SchemaKeyColumnBuilder extends SchemaColumnBuilder<SchemaKeyColumnBuilder> {
    SchemaKeyColumnBuilder affinityColumn();

    SchemaKeyBuilder done();
}
