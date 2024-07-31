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
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.UUID;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleFormatException;
import org.apache.ignite.internal.binarytuple.BinaryTupleParser;
import org.apache.ignite.internal.binarytuple.BinaryTupleParser.Sink;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Utility to convert {@link BinaryRow} to {@link BinaryTuple} with specified columns set.
 */
public class BinaryRowConverter implements ColumnsExtractor {

    private final BinaryTupleSchema srcSchema;
    private final BinaryTupleSchema dstSchema;

    /**
     * Constructor.
     *
     * @param srcSchema Source tuple schema.
     * @param dstSchema Destination tuple schema.
     */
    public BinaryRowConverter(BinaryTupleSchema srcSchema, BinaryTupleSchema dstSchema) {
        this.srcSchema = srcSchema;
        this.dstSchema = dstSchema;
    }

    /**
     * Convert a binary row to a binary tuple.
     *
     * @param row Binary row.
     * @return Binary tuple.
     */
    @Override
    public BinaryTuple extractColumns(BinaryRow row) {
        assert srcSchema.convertible();

        ByteBuffer tupleBuffer = row.tupleSlice();
        var parser = new BinaryTupleParser(srcSchema.elementCount(), tupleBuffer);

        // Estimate total data size.
        var stats = new Sink() {
            int estimatedValueSize = 0;

            @Override
            public void nextElement(int index, int begin, int end) {
                estimatedValueSize += end - begin;
            }
        };

        for (int elementIndex = 0; elementIndex < dstSchema.elementCount(); elementIndex++) {
            int columnIndex = dstSchema.columnIndex(elementIndex);
            parser.fetch(columnIndex, stats);
        }

        // Now compose the tuple.
        BinaryTupleBuilder builder = new BinaryTupleBuilder(dstSchema.elementCount(), stats.estimatedValueSize);

        for (int elementIndex = 0; elementIndex < dstSchema.elementCount(); elementIndex++) {
            int columnIndex = dstSchema.columnIndex(elementIndex);
            parser.fetch(columnIndex, (index, begin, end) -> {
                if (begin == end) {
                    builder.appendNull();
                } else {
                    builder.appendElementBytes(tupleBuffer, begin, end - begin);
                }
            });
        }
        return new BinaryTuple(dstSchema.elementCount(), builder.build());
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
        if (value == null) {
            if (!element.nullable()) {
                throw new BinaryTupleFormatException("NULL value for non-nullable column in binary tuple builder.");
            }
            return builder.appendNull();
        }

        switch (element.typeSpec()) {
            case BOOLEAN:
                return builder.appendBoolean((boolean) value);
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
            case DECIMAL:
                return builder.appendDecimalNotNull((BigDecimal) value, element.decimalScale());
            case UUID:
                return builder.appendUuidNotNull((UUID) value);
            case BYTES:
                return builder.appendBytesNotNull((byte[]) value);
            case STRING:
                return builder.appendStringNotNull((String) value);
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

    /**
     * Returns destination tuple schema.
     */
    @TestOnly
    public BinaryTupleSchema dstSchema() {
        return dstSchema;
    }

    /** Helper method to convert from a full row or key-only row to the key-only tuple. */
    public static BinaryRowConverter keyExtractor(SchemaDescriptor schema) {
        BinaryTupleSchema rowSchema = BinaryTupleSchema.createRowSchema(schema);
        BinaryTupleSchema keySchema = BinaryTupleSchema.createDestinationKeySchema(schema);

        return new BinaryRowConverter(rowSchema, keySchema);
    }

    /** Helper method to convert from a full row or key-only row to the tuple with specified columns. */
    public static BinaryRowConverter columnsExtractor(SchemaDescriptor schema, int... columns) {
        BinaryTupleSchema rowSchema = BinaryTupleSchema.createRowSchema(schema);
        BinaryTupleSchema trimmedSchema = BinaryTupleSchema.createSchema(schema, columns);

        return new BinaryRowConverter(rowSchema, trimmedSchema);
    }
}
