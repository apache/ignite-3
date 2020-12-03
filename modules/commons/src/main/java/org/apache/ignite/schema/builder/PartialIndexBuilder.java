package org.apache.ignite.schema.builder;

public interface PartialIndexBuilder extends SortedIndexBuilder {
    /** {@inheritDoc} */
    @Override PartialIndexBuilder withName(String indexName);

    /** {@inheritDoc} */
    @Override PartialIndexBuilder inlineSize(int inlineSize);

    /**
     * @param expr Partial index expression.
     * @return {@code this} for chaining.
     */
    PartialIndexBuilder expr(String expr);

    /** {@inheritDoc} */
    @Override PartialIndexColumnBuilder addIndexColumn(String name);

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
