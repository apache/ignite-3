package org.apache.ignite.internal.schema.builder;

import org.apache.ignite.internal.schema.HashIndexImpl;
import org.apache.ignite.schema.HashIndex;
import org.apache.ignite.schema.builder.HashIndexBuilder;

public class HashIndexBuilderImpl extends AbstractIndexBuilder implements HashIndexBuilder {
    protected String[] columns;

    public HashIndexBuilderImpl(String indexName) {
        super(indexName);
    }

    @Override public HashIndexBuilder withColumns(String... columns) {
        this.columns = columns.clone();

        return this;
    }

    @Override public HashIndex build() {
        assert columns != null;
        assert columns.length > 0;

        return new HashIndexImpl(name());
    }

    String[] columns() {
        return columns;
    }
}
