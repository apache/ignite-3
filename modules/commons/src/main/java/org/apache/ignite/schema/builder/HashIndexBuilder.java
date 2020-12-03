package org.apache.ignite.schema.builder;

public interface HashIndexBuilder extends IndexBuilder {
    /** {@inheritDoc} */
    @Override HashIndexBuilder withName(String indexName);

    /**
     * @param columns Indexed columns.
     * @return {@code this} for chaining.
     */
    HashIndexBuilder columns(String... columns);
}
