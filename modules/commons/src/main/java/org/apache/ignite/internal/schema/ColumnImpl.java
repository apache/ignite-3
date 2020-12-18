package org.apache.ignite.internal.schema;

import org.apache.ignite.schema.Column;
import org.apache.ignite.schema.ColumnType;

public class ColumnImpl implements Column {

    private final String name;

    public ColumnImpl(String name) {
        this.name = name;
    }

    @Override public ColumnType type() {
        return null;
    }

    @Override public String name() {
        return name;
    }

    @Override public boolean nullable() {
        return false;
    }

    @Override public Object defaultValue() {
        return null;
    }

    @Override public boolean isKeyColumn() {
        return false;
    }

    @Override public boolean isAffinityColumn() {
        return false;
    }
}
