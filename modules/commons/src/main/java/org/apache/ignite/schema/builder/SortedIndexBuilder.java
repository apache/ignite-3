package org.apache.ignite.schema.builder;

import org.apache.ignite.schema.SortedIndex;

/**
 * Sorted index descriptor builder.
 */
public interface SortedIndexBuilder extends Builder {
    /**
     * Sets inline size for index.
     *
     * @param inlineSize Index max inline size.
     * @return {@code this} for chaining.
     */
    SortedIndexBuilder withInlineSize(int inlineSize);

    /**
     * Adds column to index.
     *
     * @param name Table column name.
     * @return Index builder.
     */
    SortedIndexColumnBuilder addIndexColumn(String name);

    /**
     * @return Sorted index.
     */
    @Override SortedIndex build();

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
         * @return Parent builder for chaning.
         */
        SortedIndexBuilder done();
    }
}
