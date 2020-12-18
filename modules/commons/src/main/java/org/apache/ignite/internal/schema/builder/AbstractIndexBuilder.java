package org.apache.ignite.internal.schema.builder;

import org.apache.ignite.schema.builder.Builder;

public abstract class AbstractIndexBuilder implements Builder {
    private String name;

    AbstractIndexBuilder(String indexName) {
        this.name = indexName;
    }

    String name() {
        return name;
    }
}
