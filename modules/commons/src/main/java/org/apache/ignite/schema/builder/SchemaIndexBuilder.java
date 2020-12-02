package org.apache.ignite.schema.builder;

public interface SchemaIndexBuilder {
    SchemaIndexBuilder withName(String name);

    SchemaIndexBuilder inlineSize(int inlineSize);

    SchemaIndexColumnBuilder addIndexColumn(String name);

    SchemaBuilder done();
}
