package org.apache.ignite.schema;

public interface SchemaIndexColumnBuilder {
    SchemaIndexColumnBuilder desc();

    SchemaIndexColumnBuilder asc();

    SchemaIndexColumnBuilder withName(String name);

    SchemaIndexBuilder done();
}
