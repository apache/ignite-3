package org.apache.ignite.schema.builder;

import org.apache.ignite.schema.HashIndex;

/**
 * Hash index dscriptor builder.
 */
public interface HashIndexBuilder extends Builder {
    /**
     * @param columns Indexed columns.
     * @return {@code this} for chaining.
     */
    HashIndexBuilder withColumns(String... columns);

    /**
     * @return Hash index.
     */
    @Override HashIndex build();
}
