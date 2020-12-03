package org.apache.ignite.schema.builder;

public interface IndexBuilder {
    /**
     * @param indexName Index name.
     * @return {@code this} for chaining.
     */
    IndexBuilder withName(String indexName);

    /**
     * @return Parent builder.
     */
    SchemaTableBuilder done();
}
