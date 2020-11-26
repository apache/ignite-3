package org.apache.ignite.commons.schema;

public class IndexColumnConfigurationBuilder {
    private final IndexConfigurationBuilder indexBuilder;

    private String name;
    private boolean desc;

    public IndexColumnConfigurationBuilder(IndexConfigurationBuilder indexBuilder) {
        this.indexBuilder = indexBuilder;
    }


    public IndexColumnConfigurationBuilder desc() {
        desc = true;

        return this;
    }

    public IndexColumnConfigurationBuilder asc() {
        desc = false;

        return this;
    }

    IndexColumnConfigurationBuilder withName(String name) {
        this.name = name;

        return this;
    }

    public String name() {
        return name;
    }

    public IndexConfigurationBuilder done() {
        indexBuilder.addIndexColumn(this);

        return indexBuilder;
    }
}
