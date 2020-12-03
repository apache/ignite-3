package org.apache.ignite.internal.schema.builder;

import org.apache.ignite.schema.builder.HashIndexBuilder;

public class HashIndexBuilderImpl extends AbstractIndexBuilder implements HashIndexBuilder {
    private String[] columns;

    public HashIndexBuilderImpl(SchemaTableBuilderImpl schemaBuilder) {
        super(schemaBuilder);
    }

    @Override public HashIndexBuilder columns(String... columns) {
        this.columns = columns.clone();

        return this;
    }

    @Override public HashIndexBuilderImpl withName(String indexName) {
        super.withName(indexName);

        return this;
    }

    @Override public SchemaTableBuilderImpl done() {
        assert columns != null;
        assert columns.length > 0;

        return super.done();
    }
}
