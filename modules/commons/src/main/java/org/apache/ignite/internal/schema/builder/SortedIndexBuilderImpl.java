package org.apache.ignite.internal.schema.builder;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.schema.SortedIndex;
import org.apache.ignite.schema.builder.SortedIndexBuilder;

public class SortedIndexBuilderImpl extends AbstractIndexBuilder implements SortedIndexBuilder {
    private final Map<String, SortedIndexColumnBuilderImpl> cols = new HashMap<>();

    private int inlineSize;

    public SortedIndexBuilderImpl(String indexName) {
        super(indexName);
    }

    @Override public SortedIndexBuilderImpl withInlineSize(int inlineSize) {
        this.inlineSize = inlineSize;

        return this;
    }

    @Override public SortedIndexColumnBuilderImpl addIndexColumn(String name) {
        return new SortedIndexColumnBuilderImpl(this).withName(name);
    }

    void addIndexColumn(SortedIndexColumnBuilderImpl bld) {
        if (cols.put(bld.name(), bld) != null)
            throw new IllegalArgumentException("Index with same name already exists: " + bld.name());
    }

    Collection<? extends SortedIndexColumnBuilderImpl> columns() {
        return cols.values();
    }

    @Override public SortedIndex build() {
        assert !cols.isEmpty();

        return null;
    }

    @SuppressWarnings("PublicInnerClass")
    public static class SortedIndexColumnBuilderImpl implements SortedIndexColumnBuilder {
        private final SortedIndexBuilderImpl indexBuilder;

        private String name;
        private boolean desc;

        public SortedIndexColumnBuilderImpl(SortedIndexBuilderImpl indexBuilder) {
            this.indexBuilder = indexBuilder;
        }

        @Override public SortedIndexColumnBuilderImpl desc() {
            desc = true;

            return this;
        }

        @Override public SortedIndexColumnBuilderImpl asc() {
            desc = false;

            return this;
        }

        @Override public SortedIndexColumnBuilderImpl withName(String name) {
            this.name = name;

            return this;
        }

        public String name() {
            return name;
        }

        @Override public SortedIndexBuilderImpl done() {
            indexBuilder.addIndexColumn(this);

            return indexBuilder;
        }
    }
}
