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

package org.apache.ignite.internal.schema;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;
import org.apache.ignite.lang.IgniteInternalException;

/**
 * Description of a binary tuple.
 */
public class BinaryTupleSchema {
    /** Size of a tuple header, in bytes. */
    public static int HEADER_SIZE = 1;

    /** Mask for size of entries in variable-length offset table. */
    public static final int VARSIZE_MASK = 0b011;

    /** Flag that indicates null map presence. */
    public static final int NULLMAP_FLAG = 0b100;

    /** Default value for UUID elements. */
    public static final UUID DEFAULT_UUID = new UUID(0, 0);

    /** Default value for Date elements (Jan 1st 1 BC). */
    public static final LocalDate DEFAULT_DATE = LocalDate.of(0, 1, 1);

    /** Default value for Time elements (00:00:00). */
    public static final LocalTime DEFAULT_TIME = LocalTime.of(0, 0);

    /** Default value for DateTime elements (Jan 1st 1 BC, 00:00:00). */
    public static final LocalDateTime DEFAULT_DATE_TIME = LocalDateTime.of(0, 1, 1, 0, 0);

    /** Default value for Timestamp elements. */
    public static final Instant DEFAULT_TIMESTAMP = Instant.EPOCH;

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
         * Constructor.
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
    }

    /** Tuple schema corresponding to a set of row columns going in a contiguous range. */
    private static final class DenseRowSchema extends BinaryTupleSchema {
        int columnBase;

        boolean fullSize;

        /**
         * Constructor.
         *
         * @param elements Tuple elements.
         * @param hasNullables True if there are any nullable tuple elements, false otherwise.
         * @param columnBase Row column matching the first tuple element.
         * @param fullSize True if the tuple contains enough elements to form a full row.
         */
        private DenseRowSchema(Element[] elements, boolean hasNullables, int columnBase, boolean fullSize) {
            super(elements, hasNullables);
            this.columnBase = columnBase;
            this.fullSize = fullSize;
        }

        /** {@inheritDoc} */
        @Override
        public int columnIndex(int index) {
            return index - columnBase;
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
         * Constructor.
         *
         * @param elements Tuple elements.
         * @param columns Row column indexes.
         * @param hasNullables True if there are any nullable tuple elements, false otherwise.
         */
        private SparseRowSchema(Element[] elements, int[] columns, boolean hasNullables) {
            super(elements, hasNullables);
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

    /** Indicates if the schema contains one or more nullable elements. */
    private final boolean hasNullables;

    /**
     * Constructor.
     *
     * @param elements Tuple elements.
     * @param hasNullables True if there are any nullable tuple elements, false otherwise.
     */
    private BinaryTupleSchema(Element[] elements, boolean hasNullables) {
        this.elements = elements;
        this.hasNullables = hasNullables;
    }

    /**
     * Create a tuple schema with specified elements.
     *
     * @param elements Tuple elements.
     * @return Tuple schema.
     */
    public static BinaryTupleSchema create(Element[] elements) {
        return new BinaryTupleSchema(elements.clone(), checkNullables(elements));
    }

    /**
     * Create a schema for binary tuples with all columns of a row.
     *
     * @param descriptor Row schema.
     * @return Tuple schema.
     */
    public static BinaryTupleSchema createRowSchema(SchemaDescriptor descriptor) {
        return createSchema(descriptor, 0, descriptor.length());
    }

    /**
     * Create a schema for binary tuples with key-only columns of a row.
     *
     * @param descriptor Row schema.
     * @return Tuple schema.
     */
    public static BinaryTupleSchema createKeySchema(SchemaDescriptor descriptor) {
        return createSchema(descriptor, 0, descriptor.keyColumns().length());
    }

    /**
     * Create a schema for binary tuples with value-only columns of a row.
     *
     * @param descriptor Row schema.
     * @return Tuple schema.
     */
    public static BinaryTupleSchema createValueSchema(SchemaDescriptor descriptor) {
        return createSchema(descriptor, descriptor.keyColumns().length(), descriptor.length());
    }

    /**
     * Create a tuple schema based on a range of row columns.
     *
     * @param descriptor Row schema.
     * @param colBegin First columns in the range.
     * @param colEnd Last column in the range (exclusive).
     * @return Tuple schema.
     */
    private static BinaryTupleSchema createSchema(SchemaDescriptor descriptor, int colBegin, int colEnd) {
        int numCols = colEnd - colBegin;

        Element[] elements = new Element[numCols];
        boolean hasNullables = false;

        for (int i = 0; i < numCols; i++) {
            Column column = descriptor.column(colBegin + i);
            boolean nullable = column.nullable();
            elements[i] = new Element(column.type(), nullable);
            hasNullables |= nullable;
        }

        boolean fullSize = (colBegin == 0
                && (colEnd == descriptor.length() || colEnd == descriptor.keyColumns().length()));

        return new DenseRowSchema(elements, hasNullables, colBegin, fullSize);
    }

    /**
     * Create a schema for binary tuples with selected row columns.
     *
     * @param descriptor Row schema.
     * @param columns Row column indexes.
     * @return Tuple schema.
     */
    public static BinaryTupleSchema createSchema(SchemaDescriptor descriptor, int[] columns) {
        Element[] elements = new Element[columns.length];
        boolean hasNullables = false;

        for (int i : columns) {
            Column column = descriptor.column(i);
            boolean nullable = column.nullable();
            elements[i] = new Element(column.type(), nullable);
            hasNullables |= nullable;
        }

        return new SparseRowSchema(elements, columns.clone(), hasNullables);
    }

    /**
     * Calculate flags for a given size of variable-length area.
     *
     * @param size Variable-length area size.
     * @return Flags value.
     */
    public static byte valueSizeToFlags(long size) {
        if (size <= 0xff) {
            return 0b00;
        }
        if (size <= 0xffff) {
            return 0b01;
        }
        if (size <= Integer.MAX_VALUE) {
            return 0b10;
        }
        throw new IgniteInternalException("Too big binary tuple size");
    }

    /**
     * Calculate the size of entry in variable-length offset table for given flags.
     *
     * @param flags Flags value.
     * @return Size of entry in variable-length offset table.
     */
    public static int flagsToEntrySize(byte flags) {
        return 1 << (flags & VARSIZE_MASK);
    }

    /**
     * Calculate the null map size.
     *
     * @param numElements Number of tuple elements.
     * @return Null map size in bytes.
     */
    public static int nullMapSize(int numElements) {
        return (numElements + 7) / 8;
    }

    /**
     * Returns the null map size in bytes if there are nullable elements, zero otherwise.
     */
    public int nullMapSize() {
        return hasNullableElements() ? nullMapSize(elementCount()) : 0;
    }

    /**
     * Returns the number of elements in the tuple.
     */
    public int elementCount() {
        return elements.length;
    }

    /**
     * Returns true if there is one or more nullable elements, false otherwise.
     */
    public boolean hasNullableElements() {
        return hasNullables;
    }

    /**
     * Returns specified element descriptor.
     */
    public Element element(int index) {
        return elements[index];
    }

    /**
     * Map a tuple element index to a column index in a row.
     *
     * @return Column index if the schema is based on a SchemaDescriptor, -1 otherwise.
     */
    public int columnIndex(int index) {
        return -1;
    }

    /**
     * Check to see if the tuple can be converted to a row.
     *
     * @return True if the tuple can be converted to a row, false otherwise.
     */
    public boolean convertible() {
        return false;
    }

    /** Check to see if there are any nullable elements in the array. */
    private static boolean checkNullables(Element[] elements) {
        for (Element element : elements) {
            if (element.nullable) {
                return true;
            }
        }
        return false;
    }
}
