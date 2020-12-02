package org.apache.ignite.schema.builder;

public interface SchemaIndexColumnBuilder {
    SchemaIndexColumnBuilder desc();

    SchemaIndexColumnBuilder asc();

    SchemaIndexColumnBuilder withName(String name);

    SchemaIndexBuilder done();
}
