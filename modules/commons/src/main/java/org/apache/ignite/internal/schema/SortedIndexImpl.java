package org.apache.ignite.internal.schema;

import java.util.Collection;
import org.apache.ignite.schema.IndexColumn;
import org.apache.ignite.schema.SortedIndex;

public class SortedIndexImpl implements SortedIndex {
    private final String name;

    public SortedIndexImpl(String name) {
        this.name = name;
    }

    @Override public int inlineSize() {
        return 0;
    }

    @Override public Collection<IndexColumn> columns() {
        return null;
    }

    @Override public Collection<IndexColumn> indexedColumns() {
        return null;
    }

    @Override public String name() {
        return name;
    }
}
