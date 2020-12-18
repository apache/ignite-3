package org.apache.ignite.internal.schema.builder;

import org.apache.ignite.schema.builder.IndexBuilder;

public abstract class AbstractIndexBuilder implements IndexBuilder {
    private String name;

    IndexBuilder withName(String indexName) {
        this.name = indexName;

        return this;
    }

    String name() {
        return name;
    }
}
