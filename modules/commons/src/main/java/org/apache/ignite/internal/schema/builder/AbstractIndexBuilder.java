package org.apache.ignite.internal.schema.builder;

import org.apache.ignite.schema.builder.IndexBuilder;

public abstract class AbstractIndexBuilder implements IndexBuilder {
    private final SchemaTableBuilderImpl schemaBuilder;
    private String name;

    AbstractIndexBuilder(SchemaTableBuilderImpl schemaBuilder) {
        this.schemaBuilder = schemaBuilder;
    }

    @Override public IndexBuilder withName(String indexName) {
        this.name = indexName;

        return this;
    }

    String name() {
        return name;
    }

    @Override public SchemaTableBuilderImpl done() {
        schemaBuilder.addIndex(this);

        return schemaBuilder;
    }
}
