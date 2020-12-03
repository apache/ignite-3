package org.apache.ignite.internal.schema.builder;

import java.util.Collection;
import org.apache.ignite.schema.builder.PartialIndexBuilder;

public class PartialIndexBuilderImpl extends SortedIndexBuilderImpl implements PartialIndexBuilder {
    private String expr;

    public PartialIndexBuilderImpl(SchemaTableBuilderImpl schemaBuilder) {
        super(schemaBuilder);
    }

    @Override public PartialIndexBuilderImpl withName(String indexName) {
        super.withName(indexName);

        return this;
    }

    @Override public PartialIndexBuilderImpl inlineSize(int inlineSize) {
        super.inlineSize(inlineSize);

        return this;
    }

    void addIndexColumn(PartialIndexColumnBuilderImpl bld) {
        super.addIndexColumn(bld);
    }

    @Override Collection<PartialIndexColumnBuilderImpl> columns() {
        return (Collection<PartialIndexColumnBuilderImpl>)super.columns();
    }

    @Override public SchemaTableBuilderImpl done() {
        return super.done();
    }

    @Override public PartialIndexBuilder expr(String expr) {
        this.expr = expr;

        return this;
    }

    @Override public PartialIndexColumnBuilderImpl addIndexColumn(String name) {
        return new PartialIndexColumnBuilderImpl(this).withName(name);
    }

    @SuppressWarnings("PublicInnerClass")
    public static class PartialIndexColumnBuilderImpl extends SortedIndexColumnBuilderImpl implements PartialIndexColumnBuilder {
        public PartialIndexColumnBuilderImpl(PartialIndexBuilderImpl indexBuilder) {
            super(indexBuilder);
        }

        @Override public PartialIndexColumnBuilderImpl desc() {
            super.desc();

            return this;
        }

        @Override public PartialIndexColumnBuilderImpl asc() {
            super.asc();

            return this;
        }

        @Override public PartialIndexColumnBuilderImpl withName(String name) {
            super.withName(name);

            return this;
        }

        @Override public PartialIndexBuilderImpl done() {
            return (PartialIndexBuilderImpl)super.done();
        }
    }
}
