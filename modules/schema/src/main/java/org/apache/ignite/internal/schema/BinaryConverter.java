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
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleFormatException;
import org.apache.ignite.internal.binarytuple.BinaryTupleParser;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.jetbrains.annotations.Nullable;

/**
 * Utility to convert between IEP-92 Binary Tuple and IEP-54 Row formats.
 */
public class BinaryConverter {
    /** Row schema. */
    private final SchemaDescriptor descriptor;

    /** Placeholder for NULL values in search bounds. */
    public static final Object NULL_BOUND = new Object();

    /** Tuple schema. */
    private final BinaryTupleSchema tupleSchema;

    /** Row wrapper that allows direct access to variable-length values. */
    private class RowHelper extends Row {
        RowHelper(SchemaDescriptor descriptor, BinaryRow row) {
            super(descriptor, row);
        }

        int getLength(int colIdx, NativeTypeSpec spec) {
            return (int) (findColumn(colIdx, spec, descriptor.isKeyColumn(colIdx)) >>> 32);
        }

        void copyData(BinaryTupleBuilder builder, int columnIndex, NativeTypeSpec spec) {
            boolean isKeyCol = descriptor.isKeyColumn(columnIndex);
            ByteBuffer slice = isKeyCol ? keySlice() : valueSlice();

            long offLen = findColumn(columnIndex, spec, isKeyCol);
            int offset = (int) offLen;
            int length = spec.fixedLength() ? descriptor.column(columnIndex).type().sizeInBytes() : (int) (offLen >>> 32);

            builder.appendElementBytes(slice, offset, length);
        }
    }

    /**
     * Constructor.
     *
     * @param descriptor Row schema.
     * @param tupleSchema Tuple schema.
     */
    public BinaryConverter(
            SchemaDescriptor descriptor,
            BinaryTupleSchema tupleSchema
    ) {
        this.descriptor = descriptor;
        this.tupleSchema = tupleSchema;
    }

    /**
     * Factory method for a row converter.
     *
     * @param descriptor Row schema.
     * @return Row converter.
     */
    public static BinaryConverter forRow(SchemaDescriptor descriptor) {
        return new BinaryConverter(descriptor, BinaryTupleSchema.createRowSchema(descriptor));
    }

    /**
     * Factory method for key converter.
     *
     * @param descriptor Row schema.
     * @return Key converter.
     */
    public static BinaryConverter forKey(SchemaDescriptor descriptor) {
        return new BinaryConverter(descriptor, BinaryTupleSchema.createKeySchema(descriptor));
    }

    /**
     * Factory method for value converter.
     *
     * @param descriptor Row schema.
     * @return Key converter.
     */
    public static BinaryConverter forValue(SchemaDescriptor descriptor) {
        return new BinaryConverter(descriptor, BinaryTupleSchema.createValueSchema(descriptor));
    }

    /**
     * Convert an IEP-54 row to a binary tuple.
     *
     * @param binaryRow Row.
     * @return Buffer with binary tuple.
     */
    public BinaryTuple toTuple(BinaryRow binaryRow) {
        RowHelper row = new RowHelper(descriptor, binaryRow);

        // See if there are any NULL values and estimate total data size. As we do not use value compression for rows
        // while do it for binary tuples, so we can overestimate the size. But in the majority of cases this should be
        // good enough to prevent byte shuffling as the tuple is built.
        boolean hasNulls = false;
        int estimatedValueSize = 0;
        for (int elementIndex = 0; elementIndex < tupleSchema.elementCount(); elementIndex++) {
            BinaryTupleSchema.Element elt = tupleSchema.element(elementIndex);
            NativeTypeSpec typeSpec = elt.typeSpec;

            int columnIndex = tupleSchema.columnIndex(elementIndex);
            if (row.hasNullValue(columnIndex, typeSpec)) {
                hasNulls = true;
            } else if (typeSpec.fixedLength()) {
                estimatedValueSize += descriptor.column(columnIndex).type().sizeInBytes();
            } else {
                estimatedValueSize += row.getLength(columnIndex, typeSpec);
            }
        }

        // Now compose the tuple.
        BinaryTupleBuilder builder = new BinaryTupleBuilder(tupleSchema.elementCount(), hasNulls, estimatedValueSize);

        for (int elementIndex = 0; elementIndex < tupleSchema.elementCount(); elementIndex++) {
            BinaryTupleSchema.Element elt = tupleSchema.element(elementIndex);
            NativeTypeSpec typeSpec = elt.typeSpec;

            int columnIndex = tupleSchema.columnIndex(elementIndex);
            if (row.hasNullValue(columnIndex, typeSpec)) {
                builder.appendNull();
                continue;
            }

            switch (typeSpec) {
                case INT8:
                    builder.appendByte(row.byteValue(columnIndex));
                    break;
                case INT16:
                    builder.appendShort(row.shortValue(columnIndex));
                    break;
                case INT32:
                    builder.appendInt(row.intValue(columnIndex));
                    break;
                case INT64:
                    builder.appendLong(row.longValue(columnIndex));
                    break;
                case FLOAT:
                    builder.appendFloat(row.floatValue(columnIndex));
                    break;
                case DOUBLE:
                    builder.appendDouble(row.doubleValue(columnIndex));
                    break;
                case NUMBER:
                    builder.appendNumberNotNull(row.numberValue(columnIndex));
                    break;
                case DECIMAL:
                    builder.appendDecimalNotNull(row.decimalValue(columnIndex), elt.decimalScale);
                    break;
                case STRING:
                case BYTES:
                case BITMASK:
                    row.copyData(builder, columnIndex, typeSpec);
                    break;
                case UUID:
                    builder.appendUuidNotNull(row.uuidValue(columnIndex));
                    break;
                case DATE:
                    builder.appendDateNotNull(row.dateValue(columnIndex));
                    break;
                case TIME:
                    builder.appendTimeNotNull(row.timeValue(columnIndex));
                    break;
                case DATETIME:
                    builder.appendDateTimeNotNull(row.dateTimeValue(columnIndex));
                    break;
                case TIMESTAMP:
                    builder.appendTimestampNotNull(row.timestampValue(columnIndex));
                    break;
                default:
                    throw new InvalidTypeException("Unexpected type value: " + typeSpec);
            }
        }

        return new BinaryTuple(tupleSchema, builder.build());
    }

    /**
     * Convert a binary tuple to an IEP-54 row.
     *
     * @param bytes Binary tuple.
     * @return Row.
     */
    public @Nullable BinaryRow fromTuple(@Nullable byte[] bytes) {
        assert tupleSchema.convertible();

        if (bytes == null) {
            return null;
        }

        return fromTuple(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN));
    }

    /**
     * Convert a binary tuple to an IEP-54 row.
     *
     * @param buffer Buffer with binary tuple.
     * @return Row.
     */
    public @Nullable BinaryRow fromTuple(@Nullable ByteBuffer buffer) {
        assert tupleSchema.convertible();

        if (buffer == null) {
            return null;
        }

        var parser = new BinaryTupleParser(tupleSchema.elementCount(), buffer);

        // Collect information needed for RowAssembler.
        var stats = new BinaryTupleParser.Sink() {
            int nonNullVarLenKeyCols = 0;
            int nonNullVarLenValCols = 0;
            int nonNullVarLenKeySize = 0;
            int nonNullVarLenValSize = 0;

            @Override
            public void nextElement(int index, int begin, int end) {
                // Skip NULL values.
                if (begin == 0) {
                    return;
                }
                // Skip fixed-size values.
                if (tupleSchema.element(index).typeSpec.fixedLength()) {
                    return;
                }

                int size = end - begin;
                if (descriptor.isKeyColumn(index)) {
                    nonNullVarLenKeyCols++;
                    nonNullVarLenKeySize += size;
                } else {
                    nonNullVarLenValCols++;
                    nonNullVarLenValSize += size;
                }
            }
        };
        parser.parse(stats);

        // Now compose the row.
        var asm = new RowAssembler(descriptor, stats.nonNullVarLenKeySize, stats.nonNullVarLenKeyCols,
                stats.nonNullVarLenValSize, stats.nonNullVarLenValCols);
        parser.parse(new BinaryTupleParser.Sink() {
            @Override
            public void nextElement(int index, int begin, int end) {
                if (begin == 0) {
                    asm.appendNull();
                    return;
                }

                BinaryTupleSchema.Element elt = tupleSchema.element(index);
                switch (elt.typeSpec) {
                    case INT8:
                        asm.appendByte(parser.byteValue(begin, end));
                        break;
                    case INT16:
                        asm.appendShort(parser.shortValue(begin, end));
                        break;
                    case INT32:
                        asm.appendInt(parser.intValue(begin, end));
                        break;
                    case INT64:
                        asm.appendLong(parser.longValue(begin, end));
                        break;
                    case FLOAT:
                        asm.appendFloat(parser.floatValue(begin, end));
                        break;
                    case DOUBLE:
                        asm.appendDouble(parser.doubleValue(begin, end));
                        break;
                    case NUMBER:
                        asm.appendNumber(parser.numberValue(begin, end));
                        break;
                    case DECIMAL:
                        asm.appendDecimal(new BigDecimal(parser.numberValue(begin, end), elt.decimalScale));
                        break;
                    case STRING:
                        asm.appendString(parser.stringValue(begin, end));
                        break;
                    case BYTES:
                        asm.appendBytes(parser.bytesValue(begin, end));
                        break;
                    case UUID:
                        asm.appendUuid(parser.uuidValue(begin, end));
                        break;
                    case BITMASK:
                        asm.appendBitmask(parser.bitmaskValue(begin, end));
                        break;
                    case DATE:
                        asm.appendDate(parser.dateValue(begin, end));
                        break;
                    case TIME:
                        asm.appendTime(parser.timeValue(begin, end));
                        break;
                    case DATETIME:
                        asm.appendDateTime(parser.dateTimeValue(begin, end));
                        break;
                    case TIMESTAMP:
                        asm.appendTimestamp(parser.timestampValue(begin, end));
                        break;
                    default:
                        throw new InvalidTypeException("Unexpected type value: " + elt.typeSpec);
                }
            }
        });

        return asm.build();
    }

    /**
     * Helper method that adds value to the binary tuple builder.
     *
     * @param builder Binary tuple builder.
     * @param element Binary schema element.
     * @param value Value to add.
     * @return Binary tuple builder.
     */
    public static BinaryTupleBuilder appendValue(BinaryTupleBuilder builder, Element element, @Nullable Object value) {
        if (value == null || value == NULL_BOUND) {
            if (!element.nullable()) {
                throw new BinaryTupleFormatException("NULL value for non-nullable column in binary tuple builder.");
            }
            return builder.appendNull();
        }

        switch (element.typeSpec()) {
            case INT8:
                return builder.appendByte((byte) value);
            case INT16:
                return builder.appendShort((short) value);
            case INT32:
                return builder.appendInt((int) value);
            case INT64:
                return builder.appendLong((long) value);
            case FLOAT:
                return builder.appendFloat((float) value);
            case DOUBLE:
                return builder.appendDouble((double) value);
            case NUMBER:
                return builder.appendNumberNotNull((BigInteger) value);
            case DECIMAL:
                return builder.appendDecimalNotNull((BigDecimal) value, element.decimalScale());
            case UUID:
                return builder.appendUuidNotNull((UUID) value);
            case BYTES:
                return builder.appendBytesNotNull((byte[]) value);
            case STRING:
                return builder.appendStringNotNull((String) value);
            case BITMASK:
                return builder.appendBitmaskNotNull((BitSet) value);
            case DATE:
                return builder.appendDateNotNull((LocalDate) value);
            case TIME:
                return builder.appendTimeNotNull((LocalTime) value);
            case DATETIME:
                return builder.appendDateTimeNotNull((LocalDateTime) value);
            case TIMESTAMP:
                return builder.appendTimestampNotNull((Instant) value);
            default:
                break;
        }

        throw new InvalidTypeException("Unexpected type value: " + element.typeSpec());
    }
}
