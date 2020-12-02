package org.apache.ignite.schema;

import java.util.Collection;

public interface TableIndex {
    String name();

    int inlineSize();

    Collection<IndexColumn> columns();

    // TODO: MUST contains all affinity columns.
    default boolean unique() {
        return false;
    }
}
