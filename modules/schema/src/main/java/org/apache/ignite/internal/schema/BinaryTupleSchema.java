/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.schema;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleFormatException;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.lang.InternalTuple;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.jetbrains.annotations.Nullable;

/**
 * Description of a binary tuple.
 */
public class BinaryTupleSchema {
    /**
     * Tuple element description used for tuple parsing and building.
     *
     * <p>For binary tuples encoding of values is determined by its basic type and the value itself. Parameters
     * like precision and scale defined for columns in schema are not taken into account. The only exception
     * is the Decimal type where the scale parameter is required for decoding.
     *
     * <p>To keep things simple we have the scale parameter everywhere but really use it only for Decimals.
     */
    public static final class Element {
        final NativeTypeSpec typeSpec;

        final int decimalScale;

        final boolean nullable;

        /**
         * Constructs a tuple element description.
         *
         * @param type Element data type.
         * @param nullable True for nullable elements, false for non-nullable.
         */
        public Element(NativeType type, boolean nullable) {
            typeSpec = type.spec();

            if (typeSpec == NativeTypeSpec.DECIMAL) {
                DecimalNativeType decimalType = (DecimalNativeType) type;
                decimalScale = decimalType.scale();
            } else {
                decimalScale = 0;
            }

            this.nullable = nullable;
        }

        /**
         * Gets the type spec.
         *
         * @return Type spec.
         */
        public NativeTypeSpec typeSpec() {
            return typeSpec;
        }

        /**
         * Gets the decimal scale.
         *
         * @return Decimal scale.
         */
        public int decimalScale() {
            return decimalScale;
        }

        /**
         * Gets the nullable flag.
         *
         * @return Nullable flag.
         */
        public boolean nullable() {
            return nullable;
        }
    }

    /** Tuple schema corresponding to a set of row columns going in a contiguous range. */
    private static final class DenseRowSchema extends BinaryTupleSchema {
        int columnBase;

        boolean fullSize;

        /**
         * Constructs a tuple schema for a contiguous range of columns.
         *
         * @param elements Tuple elements.
         * @param columnBase Row column matching the first tuple element.
         * @param fullSize True if the tuple contains enough elements to form a full row.
         */
        private DenseRowSchema(Element[] elements, int columnBase, boolean fullSize) {
            super(elements);
            this.columnBase = columnBase;
            this.fullSize = fullSize;
        }

        /** {@inheritDoc} */
        @Override
        public int columnIndex(int index) {
            return index + columnBase;
        }

        /** {@inheritDoc} */
        @Override
        public boolean convertible() {
            return fullSize;
        }
    }

    /** Tuple schema corresponding to a set of row columns going in an arbitrary order. */
    private static final class SparseRowSchema extends BinaryTupleSchema {
        int[] columns;

        /**
         * Constructs a tuple schema for an arbitrary set of columns.
         *
         * @param elements Tuple elements.
         * @param columns Row column indexes.
         */
        private SparseRowSchema(Element[] elements, int[] columns) {
            super(elements);
            this.columns = columns;
        }

        /** {@inheritDoc} */
        @Override
        public int columnIndex(int index) {
            return columns[index];
        }
    }

    /** Descriptors of all tuple elements. */
    private final Element[] elements;

    /**
     * Constructs a tuple schema object.
     *
     * @param elements Tuple elements.
     */
    private BinaryTupleSchema(Element[] elements) {
        this.elements = elements;
    }

    /**
     * Creates a tuple schema with specified elements.
     *
     * @param elements Tuple elements.
     * @return Tuple schema.
     */
    public static BinaryTupleSchema create(Element[] elements) {
        return new BinaryTupleSchema(elements.clone());
    }

    /**
     * Creates a schema for binary tuples with all columns of a row.
     *
     * @param descriptor Row schema.
     * @return Tuple schema.
     */
    public static BinaryTupleSchema createRowSchema(SchemaDescriptor descriptor) {
        return createSchema(descriptor, 0, descriptor.length());
    }

    /**
     * Creates a schema for binary tuples with key-only columns of a row.
     *
     * @param descriptor Row schema.
     * @return Tuple schema.
     */
    public static BinaryTupleSchema createKeySchema(SchemaDescriptor descriptor) {
        List<Column> columns = descriptor.keyColumns();
        Element[] elements = new Element[columns.size()];

        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            elements[i] = new Element(column.type(), column.nullable());
        }

        // Key schema can be converted into a key-only tuple, so this schema should be have convertible = true
        return new DenseRowSchema(elements, 0, true);
    }

    /**
     * Creates a schema for binary tuples that should be used to place key columns into a row.
     * Unlike {@link #createKeySchema(SchemaDescriptor)} this schema is not convertible, because
     * key columns might be located at arbitrary positions and in non-consecutive manner.
     *
     * @param descriptor Row schema.
     * @return Tuple schema.
     */
    public static BinaryTupleSchema createDestinationKeySchema(SchemaDescriptor descriptor) {
        List<Column> columns = descriptor.keyColumns();
        Element[] elements = new Element[columns.size()];
        int[] positions = new int[columns.size()];

        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            elements[i] = new Element(column.type(), column.nullable());
            positions[i] = column.positionInRow();
        }

        return new SparseRowSchema(elements, positions);
    }

    /**
     * Creates a schema for binary tuples with value-only columns of a row.
     *
     * @param descriptor Row schema.
     * @return Tuple schema.
     */
    public static BinaryTupleSchema createValueSchema(SchemaDescriptor descriptor) {
        return createSchema(descriptor, descriptor.keyColumns().size(), descriptor.length());
    }

    /**
     * Creates a tuple schema based on a range of row columns.
     *
     * @param descriptor Row schema.
     * @param colBegin First columns in the range.
     * @param colEnd Last column in the range (exclusive).
     * @return Tuple schema.
     */
    private static BinaryTupleSchema createSchema(SchemaDescriptor descriptor, int colBegin, int colEnd) {
        int numCols = colEnd - colBegin;

        Element[] elements = new Element[numCols];

        for (int i = 0; i < numCols; i++) {
            Column column = descriptor.column(colBegin + i);
            elements[i] = new Element(column.type(), column.nullable());
        }

        boolean fullSize = (colBegin == 0
                && (colEnd == descriptor.length() || colEnd == descriptor.keyColumns().size()));

        return new DenseRowSchema(elements, colBegin, fullSize);
    }

    /**
     * Creates a schema for binary tuples with selected row columns.
     *
     * @param descriptor Row schema.
     * @param columns Row column indexes.
     * @return Tuple schema.
     */
    public static BinaryTupleSchema createSchema(SchemaDescriptor descriptor, int[] columns) {
        Element[] elements = new Element[columns.length];

        for (int i = 0; i < columns.length; i++) {
            Column column = descriptor.column(columns[i]);
            elements[i] = new Element(column.type(), column.nullable());
        }

        return new SparseRowSchema(elements, columns.clone());
    }

    /**
     * Returns the number of elements in the tuple.
     */
    public int elementCount() {
        return elements.length;
    }

    /**
     * Returns specified element descriptor.
     */
    public Element element(int index) {
        return elements[index];
    }

    /**
     * Maps a tuple element index to a column index in a row.
     *
     * @return Column index if the schema is based on a SchemaDescriptor, -1 otherwise.
     */
    public int columnIndex(int index) {
        return -1;
    }

    /**
     * Tests if the tuple can be converted to a row.
     *
     * @return True if the tuple can be converted to a row, false otherwise.
     */
    public boolean convertible() {
        return false;
    }

    /**
     * Reads a {@code BigDecimal} value from the given tuple at the given field.
     *
     * @param tuple Tuple to read the value from.
     * @param index Field index.
     * @return {@code BigDecimal} value of the field.
     */
    public @Nullable BigDecimal decimalValue(BinaryTupleReader tuple, int index) {
        return tuple.decimalValue(index, element(index).decimalScale);
    }

    /**
     * Gets an Object representation from a tuple's field. This method does no type conversions and
     * will throw an exception if row column type differs from this type.
     *
     * @param tuple Tuple to read the value from.
     * @param index Field index to read.
     * @return An Object representation of the value.
     */
    public Object value(InternalTuple tuple, int index) {
        Element element = element(index);

        switch (element.typeSpec) {
            case BOOLEAN: return tuple.booleanValueBoxed(index);
            case INT8: return tuple.byteValueBoxed(index);
            case INT16: return tuple.shortValueBoxed(index);
            case INT32: return tuple.intValueBoxed(index);
            case INT64: return tuple.longValueBoxed(index);
            case FLOAT: return tuple.floatValueBoxed(index);
            case DOUBLE: return tuple.doubleValueBoxed(index);
            case DECIMAL: return tuple.decimalValue(index, element.decimalScale);
            case UUID: return tuple.uuidValue(index);
            case STRING: return tuple.stringValue(index);
            case BYTES: return tuple.bytesValue(index);
            case DATE: return tuple.dateValue(index);
            case TIME: return tuple.timeValue(index);
            case DATETIME: return tuple.dateTimeValue(index);
            case TIMESTAMP: return tuple.timestampValue(index);
            default: throw new InvalidTypeException("Unknown element type: " + element.typeSpec);
        }
    }

    /**
     * Helper method that adds value to the binary tuple builder.
     *
     * @param builder Binary tuple builder.
     * @param index Field index to write.
     * @param value Value to add.
     * @return Binary tuple builder.
     */
    public BinaryTupleBuilder appendValue(BinaryTupleBuilder builder, int index, @Nullable Object value) {
        Element element = element(index);

        if (value == null) {
            if (!element.nullable()) {
                throw new BinaryTupleFormatException("NULL value for non-nullable column in binary tuple builder.");
            }

            return builder.appendNull();
        }

        switch (element.typeSpec()) {
            case BOOLEAN: return builder.appendBoolean((boolean) value);
            case INT8: return builder.appendByte((byte) value);
            case INT16: return builder.appendShort((short) value);
            case INT32: return builder.appendInt((int) value);
            case INT64: return builder.appendLong((long) value);
            case FLOAT: return builder.appendFloat((float) value);
            case DOUBLE: return builder.appendDouble((double) value);
            case DECIMAL: return builder.appendDecimalNotNull((BigDecimal) value, element.decimalScale());
            case UUID: return builder.appendUuidNotNull((UUID) value);
            case BYTES: return builder.appendBytesNotNull((byte[]) value);
            case STRING: return builder.appendStringNotNull((String) value);
            case DATE: return builder.appendDateNotNull((LocalDate) value);
            case TIME: return builder.appendTimeNotNull((LocalTime) value);
            case DATETIME: return builder.appendDateTimeNotNull((LocalDateTime) value);
            case TIMESTAMP: return builder.appendTimestampNotNull((Instant) value);
            default: throw new InvalidTypeException("Unknown element type: " + element.typeSpec);
        }
    }
}
