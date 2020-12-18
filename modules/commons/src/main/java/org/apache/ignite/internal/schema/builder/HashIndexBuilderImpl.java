package org.apache.ignite.internal.schema.builder;

import org.apache.ignite.schema.HashIndex;
import org.apache.ignite.schema.builder.HashIndexBuilder;

public class HashIndexBuilderImpl extends AbstractIndexBuilder implements HashIndexBuilder {
    protected String[] columns;

    @Override public HashIndexBuilderImpl withName(String indexName) {
        super.withName(indexName);

        return this;
    }

    @Override public HashIndexBuilder withColumns(String... columns) {
        this.columns = columns.clone();

        return this;
    }

    @Override public HashIndex build() {
        assert columns != null;
        assert columns.length > 0;

        return null;
    }

    String[] columns() {
        return columns;
    }
}
