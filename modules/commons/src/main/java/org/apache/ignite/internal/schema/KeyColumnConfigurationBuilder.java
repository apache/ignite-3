package org.apache.ignite.internal.schema;

import org.apache.ignite.schema.SchemaKeyColumnBuilder;

public class KeyColumnConfigurationBuilder extends ColumnConfigurationBuilder<SchemaKeyColumnBuilder> implements SchemaKeyColumnBuilder {
    private final KeyColumnCollectionBuilder parent;
    private boolean isAffinityColumn = false;

    public KeyColumnConfigurationBuilder(KeyColumnCollectionBuilder parent) {
        this.parent = parent;
    }

    @Override public KeyColumnConfigurationBuilder affinityColumn() {
        isAffinityColumn = true;

        return this;
    }

    public boolean isAffinityColumn() {
        return isAffinityColumn;
    }

    @Override public KeyColumnCollectionBuilder done() {
        parent.addColumn(this);

        return parent;
    }

    @Override protected SchemaKeyColumnBuilder getThis() {
        return this;
    }
}
