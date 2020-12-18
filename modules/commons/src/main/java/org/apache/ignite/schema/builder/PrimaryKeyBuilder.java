package org.apache.ignite.schema.builder;

public interface PrimaryKeyBuilder {
    /**
     * @param columns Indexed columns.
     * @return {@code this} for chaining.
     */
    PrimaryKeyBuilder withColumns(String... columns);

    /**
     * @param colNames Column names.
     * @return {@code this} for chaining.
     */
    PrimaryKeyBuilder withAffinityColumns(String... colNames);

    /**
     * @return Parent builder for chaning.
     */
    SchemaTableBuilder done();
}
