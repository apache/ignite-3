package org.apache.ignite.internal.schema;

import org.apache.ignite.schema.SchemaIndexColumnBuilder;

public class IndexColumnConfigurationBuilder implements SchemaIndexColumnBuilder {
    private final IndexConfigurationBuilder indexBuilder;

    private String name;
    private boolean desc;

    public IndexColumnConfigurationBuilder(IndexConfigurationBuilder indexBuilder) {
        this.indexBuilder = indexBuilder;
    }

    @Override public IndexColumnConfigurationBuilder desc() {
        desc = true;

        return this;
    }

    @Override public IndexColumnConfigurationBuilder asc() {
        desc = false;

        return this;
    }

    @Override public IndexColumnConfigurationBuilder withName(String name) {
        this.name = name;

        return this;
    }

    public String name() {
        return name;
    }

    @Override public IndexConfigurationBuilder done() {
        indexBuilder.addIndexColumn(this);

        return indexBuilder;
    }
}
