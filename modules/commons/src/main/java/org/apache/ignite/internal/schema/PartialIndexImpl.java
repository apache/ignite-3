package org.apache.ignite.internal.schema;

import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.schema.IndexColumn;
import org.apache.ignite.schema.PartialIndex;

public class PartialIndexImpl implements PartialIndex {
    private final String name;

    public PartialIndexImpl(String name) {
        this.name = name;
    }

    @Override public String expr() {
        return null;
    }

    @Override public int inlineSize() {
        return 0;
    }

    @Override public Collection<IndexColumn> columns() {
        return Collections.emptyList();
    }

    @Override public Collection<IndexColumn> indexedColumns() {
        return Collections.emptyList();
    }

    @Override public String name() {
        return name;
    }
}
