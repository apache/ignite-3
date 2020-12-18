package org.apache.ignite.schema.builder;

import org.apache.ignite.schema.PartialIndex;

public interface PartialIndexBuilder extends SortedIndexBuilder {
    /** {@inheritDoc} */
    @Override PartialIndexBuilder withInlineSize(int inlineSize);

    /**
     * @param expr Partial index expression.
     * @return {@code this} for chaining.
     */
    PartialIndexBuilder withExpression(String expr);

    /** {@inheritDoc} */
    @Override PartialIndexColumnBuilder addIndexColumn(String name);

    /**
     * @return Partial index.
     */
    @Override PartialIndex build();

    @SuppressWarnings("PublicInnerClass")
    interface PartialIndexColumnBuilder extends SortedIndexColumnBuilder {
        /** {@inheritDoc} */
        @Override PartialIndexColumnBuilder desc();

        /** {@inheritDoc} */
        @Override PartialIndexColumnBuilder asc();

        /** {@inheritDoc} */
        @Override PartialIndexColumnBuilder withName(String name);

        /** {@inheritDoc} */
        @Override PartialIndexBuilder done();
    }
}
