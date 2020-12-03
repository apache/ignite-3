package org.apache.ignite.schema.builder;

public interface SortedIndexBuilder extends IndexBuilder {
    /** {@inheritDoc} */
    @Override SortedIndexBuilder withName(String indexName);

    /**
     * @param inlineSize Index max inline size.
     * @return {@code this} for chaining.
     */
    SortedIndexBuilder inlineSize(int inlineSize);

    /**
     * @param name Table column name.
     * @return Index builder.
     */
    SortedIndexColumnBuilder addIndexColumn(String name);

    @SuppressWarnings("PublicInnerClass")
    interface SortedIndexColumnBuilder {
        /**
         * Sets descending sort order.
         *
         * @return {@code this} for chaining.
         */
        SortedIndexColumnBuilder desc();

        /**
         * Sets ascending sort order.
         *
         * @return {@code this} for chaining.
         */
        SortedIndexColumnBuilder asc();

        /**
         * @param name Column name.
         * @return {@code this} for chaining.
         */
        SortedIndexColumnBuilder withName(String name);

        /**
         * @return Parent builder.
         */
        SortedIndexBuilder done();
    }
}
