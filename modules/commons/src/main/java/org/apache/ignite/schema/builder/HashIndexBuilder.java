package org.apache.ignite.schema.builder;

public interface HashIndexBuilder extends IndexBuilder {
    /**
     * @param columns Indexed columns.
     * @return {@code this} for chaining.
     */
    HashIndexBuilder withColumns(String... columns);
}
