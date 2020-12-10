/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.schema.marshaller.reflection;

import java.lang.reflect.Field;
import java.util.Objects;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.Tuple;
import org.apache.ignite.internal.schema.TupleAssembler;
import org.apache.ignite.internal.schema.marshaller.BinaryMode;
import org.apache.ignite.internal.schema.marshaller.MarshallerUtil;
import org.apache.ignite.internal.schema.marshaller.SerializationException;
import org.apache.ignite.internal.util.IgniteUnsafeUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Field accessor to speedup access.
 */
// TODO: Extract interface, move to java-8 profile and add Java9+ implementation using VarHandles.
public abstract class UnsafeFieldAccessor {
    /**
     * TODO: implement sesitive information filtering.
     *
     * @return {@code False} if sensitive information exoising is prohibited, {@code false} otherwise.
     */
    private static boolean includeSensitive() {
        return true;
    }

    /** Offset. */
    protected final long offset;

    /** Mode. */
    protected final BinaryMode mode;

    /** Field name */
    protected final String name;

    /**
     * Mapped column position in schema.
     * <p>
     * NODE: Do not mix up with column index in {@link Columns} container.
     */
    protected final int colIdx;

    /**
     * Create accessor for the field.
     *
     * @param type Object class.
     * @param col Mapped column.
     * @param colIdx Column index in schema.
     * @return Accessor.
     */
    static UnsafeFieldAccessor create(Class<?> type, Column col, int colIdx) {
        try {
            final Field field = type.getDeclaredField(col.name());

            if (field.getType().isPrimitive() && col.nullable())
                throw new IllegalArgumentException("Failed to map non-nullable field to nullable column [name=" + field.getName() + ']');

            BinaryMode mode = MarshallerUtil.mode(field.getType());

            switch (mode) {
                case P_BYTE:
                    return new BytePrimitiveAccessor(field, colIdx);

                case P_SHORT:
                    return new ShortPrimitiveAccessor(field, colIdx);

                case P_INT:
                    return new IntPrimitiveAccessor(field, colIdx);

                case P_LONG:
                    return new LongPrimitiveAccessor(field, colIdx);

                case P_FLOAT:
                    return new FloatPrimitiveAccessor(field, colIdx);

                case P_DOUBLE:
                    return new DoublePrimitiveAccessor(field, colIdx);

                case BYTE:
                case SHORT:
                case INT:
                case LONG:
                case FLOAT:
                case DOUBLE:
                case STRING:
                case UUID:
                case BYTE_ARR:
                case BITSET:
                    return new ReferenceFieldAccessor(field, colIdx, mode);

                default:
                    assert false : "Invalid mode " + mode;
            }

            throw new IllegalArgumentException("Failed to create accessor for field [name=" + field.getName() + ']');
        }
        catch (NoSuchFieldException | SecurityException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    /**
     * Create accessor for the field.
     *
     * @param col Column.
     * @param colIdx Column index.
     * @param mode Binary mode.
     * @return Accessor.
     */
    static UnsafeFieldAccessor createIdentityAccessor(Column col, int colIdx, BinaryMode mode) {
        switch (mode) {
            //  Marshaller read/write object contract methods allowed boxed types only.
            case P_BYTE:
            case P_SHORT:
            case P_INT:
            case P_LONG:
            case P_FLOAT:
            case P_DOUBLE:
                throw new IllegalArgumentException("Primitive key/value types are not possible by API contract.");

            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case STRING:
            case UUID:
            case BYTE_ARR:
            case BITSET:
                return new IdentityAccessor(colIdx, mode);

            default:
                assert false : "Invalid mode " + mode;
        }

        throw new IllegalArgumentException("Failed to create accessor for column [name=" + col.name() + ']');
    }

    /**
     * Protected constructor.
     *
     * @param field Field.
     * @param colIdx Column index.
     * @param mode Binary mode;
     */
    protected UnsafeFieldAccessor(Field field, int colIdx, BinaryMode mode) {
        assert field != null;
        assert colIdx >= 0;
        assert mode != null;

        this.colIdx = colIdx;
        this.mode = mode;
        offset = IgniteUnsafeUtils.objectFieldOffset(field);
        name = field.getName();
    }

    /**
     * Protected constructor.
     *
     * @param colIdx Column index.
     * @param mode Binary mode;
     */
    private UnsafeFieldAccessor(int colIdx, BinaryMode mode) {
        assert colIdx >= 0;
        assert mode != null;

        this.colIdx = colIdx;
        this.mode = mode;
        offset = 0;
        name = null;
    }

    /**
     * Get binary read/write mode.
     *
     * @return Binary mode.
     */
    public BinaryMode mode() {
        return mode;
    }

    /**
     * Write object field value to tuple.
     *
     * @param obj Source object.
     * @param writer Tuple writer.
     * @throws SerializationException If failed.
     */
    public void write(Object obj, TupleAssembler writer) throws SerializationException {
        try {
            write0(Objects.requireNonNull(obj), writer);
        }
        catch (Exception ex) {
            if (includeSensitive() && name != null)
                throw new SerializationException("Failed to read field [id=" + colIdx + ']', ex);
            else
                throw new SerializationException("Failed to write field [id=" + colIdx + ']', ex);
        }
    }

    /**
     * Write object field value to tuple.
     *
     * @param obj Source object.
     * @param writer Tuple writer.
     * @throws IllegalAccessException If failed.
     */
    protected abstract void write0(Object obj, TupleAssembler writer) throws IllegalAccessException;

    /**
     * Reads value fom tuple to object field.
     *
     * @param obj Target object.
     * @param reader Tuple reader.
     * @throws SerializationException If failed.
     */
    public void read(Object obj, Tuple reader) throws SerializationException {
        try {
            read0(Objects.requireNonNull(obj), reader);
        }
        catch (Exception ex) {
            if (includeSensitive() && name != null)
                throw new SerializationException("Failed to read field [name=" + name + ']', ex);
            else
                throw new SerializationException("Failed to read field [id=" + colIdx + ']', ex);
        }
    }

    /**
     * Reads value fom tuple to object field.
     *
     * @param obj Target object.
     * @param reader Tuple reader.
     * @throws IllegalAccessException If failed.
     */
    protected abstract void read0(Object obj, Tuple reader) throws IllegalAccessException;

    /**
     * Read value.
     *
     * @param reader Tuple reader.
     * @return Object.
     */
    public Object read(Tuple reader) {
        throw new UnsupportedOperationException();
    }

    /**
     * Reads object field value.
     *
     * @param obj Object.
     * @return Field value of given object.
     */
    @Nullable Object value(Object obj) {
        return IgniteUnsafeUtils.getObjectField(Objects.requireNonNull(obj), offset);
    }

    /**
     * Accessor for field of primitive {@code byte} type.
     */
    private static class IdentityAccessor extends UnsafeFieldAccessor {
        /**
         * Constructor.
         *
         * @param colIdx Column index.
         * @param mode Binary mode.
         */
        public IdentityAccessor(int colIdx, BinaryMode mode) {
            super(colIdx, mode);
        }

        /** {@inheritDoc} */
        @Override protected void write0(Object obj, TupleAssembler writer) {
            JavaSerializer.writeRefObject(Objects.requireNonNull(obj, "Null values are not supported."), writer, mode);
        }

        /** {@inheritDoc} */
        @Override protected void read0(Object obj, Tuple reader) {
            throw new UnsupportedOperationException("Called identity accessor for object field.");
        }

        /** {@inheritDoc} */
        @Override public Object read(Tuple reader) {
            return JavaSerializer.readRefValue(reader, colIdx, mode);
        }

        /** {@inheritDoc} */
        @Override @Nullable Object value(Object obj) {
            return obj;
        }
    }

    /**
     * Accessor for field of primitive {@code byte} type.
     */
    private static class BytePrimitiveAccessor extends UnsafeFieldAccessor {
        /**
         * Constructor.
         *
         * @param field Field.
         * @param colIdx Column index.
         */
        public BytePrimitiveAccessor(Field field, int colIdx) {
            super(field, colIdx, BinaryMode.P_BYTE);
        }

        /** {@inheritDoc} */
        @Override protected void write0(Object obj, TupleAssembler writer) {
            final byte val = IgniteUnsafeUtils.getByteField(obj, offset);

            writer.appendByte(val);
        }

        /** {@inheritDoc} */
        @Override protected void read0(Object obj, Tuple reader) {
            final byte val = reader.byteValue(colIdx);

            IgniteUnsafeUtils.putByteField(obj, offset, val);
        }
    }

    /**
     * Accessor for field of primitive {@code short} type.
     */
    private static class ShortPrimitiveAccessor extends UnsafeFieldAccessor {
        /**
         * Constructor.
         *
         * @param field Field.
         * @param colIdx Column index.
         */
        public ShortPrimitiveAccessor(Field field, int colIdx) {
            super(field, colIdx, BinaryMode.P_SHORT);
        }

        /** {@inheritDoc} */
        @Override protected void write0(Object obj, TupleAssembler writer) {
            final short val = IgniteUnsafeUtils.getShortField(obj, offset);

            writer.appendShort(val);
        }

        /** {@inheritDoc} */
        @Override protected void read0(Object obj, Tuple reader) {
            final short val = reader.shortValue(colIdx);

            IgniteUnsafeUtils.putShortField(obj, offset, val);
        }
    }

    /**
     * Accessor for field of primitive {@code int} type.
     */
    private static class IntPrimitiveAccessor extends UnsafeFieldAccessor {
        /**
         * Constructor.
         *
         * @param field Field.
         * @param colIdx Column index.
         */
        public IntPrimitiveAccessor(Field field, int colIdx) {
            super(field, colIdx, BinaryMode.P_INT);
        }

        /** {@inheritDoc} */
        @Override protected void write0(Object obj, TupleAssembler writer) {
            final int val = IgniteUnsafeUtils.getIntField(obj, offset);

            writer.appendInt(val);
        }

        /** {@inheritDoc} */
        @Override protected void read0(Object obj, Tuple reader) {
            final int val = reader.intValue(colIdx);

            IgniteUnsafeUtils.putIntField(obj, offset, val);
        }
    }

    /**
     * Accessor for field of primitive {@code long} type.
     */
    private static class LongPrimitiveAccessor extends UnsafeFieldAccessor {
        /**
         * Constructor.
         *
         * @param field Field.
         * @param colIdx Column index.
         */
        public LongPrimitiveAccessor(Field field, int colIdx) {
            super(field, colIdx, BinaryMode.P_LONG);
        }

        /** {@inheritDoc} */
        @Override protected void write0(Object obj, TupleAssembler writer) {
            final long val = IgniteUnsafeUtils.getLongField(obj, offset);

            writer.appendLong(val);
        }

        /** {@inheritDoc} */
        @Override protected void read0(Object obj, Tuple reader) {
            final long val = reader.longValue(colIdx);

            IgniteUnsafeUtils.putLongField(obj, offset, val);
        }
    }

    /**
     * Accessor for field of primitive {@code float} type.
     */
    private static class FloatPrimitiveAccessor extends UnsafeFieldAccessor {
        /**
         * Constructor.
         *
         * @param field Field.
         * @param colIdx Column index.
         */
        public FloatPrimitiveAccessor(Field field, int colIdx) {
            super(field, colIdx, BinaryMode.P_FLOAT);
        }

        /** {@inheritDoc} */
        @Override protected void write0(Object obj, TupleAssembler writer) {
            final float val = IgniteUnsafeUtils.getFloatField(obj, offset);

            writer.appendFloat(val);
        }

        /** {@inheritDoc} */
        @Override protected void read0(Object obj, Tuple reader) {
            final float val = reader.floatValue(colIdx);

            IgniteUnsafeUtils.putFloatField(obj, offset, val);
        }
    }

    /**
     * Accessor for field of primitive {@code double} type.
     */
    private static class DoublePrimitiveAccessor extends UnsafeFieldAccessor {
        /**
         * Constructor.
         *
         * @param field Field.
         * @param colIdx Column index.
         */
        public DoublePrimitiveAccessor(Field field, int colIdx) {
            super(field, colIdx, BinaryMode.P_DOUBLE);
        }

        /** {@inheritDoc} */
        @Override protected void write0(Object obj, TupleAssembler writer) {
            final double val = IgniteUnsafeUtils.getDoubleField(obj, offset);

            writer.appendDouble(val);
        }

        /** {@inheritDoc} */
        @Override protected void read0(Object obj, Tuple reader) {
            final double val = reader.doubleValue(colIdx);

            IgniteUnsafeUtils.putDoubleField(obj, offset, val);
        }
    }

    /**
     * Accessor for field of reference type.
     */
    private static class ReferenceFieldAccessor extends UnsafeFieldAccessor {
        /**
         * Constructor.
         *
         * @param field Field.
         * @param colIdx Column index.
         * @param mode Binary mode.
         */
        ReferenceFieldAccessor(Field field, int colIdx, BinaryMode mode) {
            super(field, colIdx, mode);
        }

        /** {@inheritDoc} */
        @Override protected void write0(Object obj, TupleAssembler writer) {
            assert obj != null;
            assert writer != null;

            Object val;

            val = IgniteUnsafeUtils.getObjectField(obj, offset);

            if (val == null) {
                writer.appendNull();

                return;
            }

            JavaSerializer.writeRefObject(val, writer, mode);
        }

        /** {@inheritDoc} */
        @Override public void read0(Object obj, Tuple reader) {
            Object val = JavaSerializer.readRefValue(reader, colIdx, mode);

            IgniteUnsafeUtils.putObjectField(obj, offset, val);
        }
    }
}
