package org.apache.ignite.internal.sql.engine.planner.util;

import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.DefaultValueStrategy;

public class TestColumnDescriptor implements ColumnDescriptor {
    private final int idx;

    private final String name;

    private final NativeType physicalType;

    TestColumnDescriptor(int idx, String name, NativeType physicalType) {
        this.idx = idx;
        this.name = name;
        this.physicalType = physicalType;
    }

    /** {@inheritDoc} */
    @Override
    public boolean nullable() {
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public boolean key() {
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public DefaultValueStrategy defaultStrategy() {
        return DefaultValueStrategy.DEFAULT_NULL;
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override
    public int logicalIndex() {
        return idx;
    }

    /** {@inheritDoc} */
    @Override
    public int physicalIndex() {
        return idx;
    }

    /** {@inheritDoc} */
    @Override
    public NativeType physicalType() {
        if (physicalType == null) {
            throw new AssertionError();
        }

        return physicalType;
    }

    /** {@inheritDoc} */
    @Override
    public Object defaultValue() {
        throw new AssertionError();
    }
}
