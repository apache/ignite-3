package org.apache.ignite.internal.processors.query.calcite.schema;

import java.util.function.Supplier;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.processors.query.calcite.type.IgniteTypeFactory;
import org.jetbrains.annotations.Nullable;

/** */
public class ColumnDescriptorImpl implements ColumnDescriptor {
    private final boolean key;

    private final String name;

    /** */
    private final @Nullable Supplier<Object> dfltVal;

    /** */
    private final int fieldIdx;

    /** */
    private final Class<?> storageType;

    public ColumnDescriptorImpl(
        String name,
        boolean key,
        int fieldIdx,
        Class<?> storageType,
        @Nullable Supplier<Object> dfltVal
    ) {
        this.key = key;
        this.name = name;
        this.dfltVal = dfltVal;
        this.fieldIdx = fieldIdx;
        this.storageType = storageType;
    }

    /** {@inheritDoc} */
    @Override public boolean key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public boolean hasDefaultValue() {
        return dfltVal != null;
    }

    /** {@inheritDoc} */
    @Override public Object defaultValue() {
        return dfltVal != null ? dfltVal.get() : null;
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return name;
    }

    /** {@inheritDoc} */
    @Override public int fieldIndex() {
        return fieldIdx;
    }

    /** {@inheritDoc} */
    @Override public RelDataType logicalType(IgniteTypeFactory f) {
        return f.createJavaType(storageType);
    }

    /** {@inheritDoc} */
    @Override public Class<?> storageType() {
        return storageType;
    }

    /** {@inheritDoc} */
    @Override public void set(Object dst, Object val) {
        throw new AssertionError();
    }
}
