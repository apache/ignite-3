/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.storage.index.impl;

import static java.util.stream.Collectors.toUnmodifiableList;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.SchemaMismatchException;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.index.HashIndexDescriptor;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.SortedIndexDescriptor;

/**
 * Class for converting an array of objects into a {@link BinaryTuple} and vice-versa using a given index schema.
 */
public class BinaryTupleRowSerializer {
    private static class ColumnDescriptor {
        final NativeType type;

        final boolean nullable;

        ColumnDescriptor(NativeType type, boolean nullable) {
            this.type = type;
            this.nullable = nullable;
        }
    }

    private final List<ColumnDescriptor> schema;

    /**
     * Creates a new instance for a Sorted Index.
     */
    public BinaryTupleRowSerializer(SortedIndexDescriptor descriptor) {
        this.schema = descriptor.indexColumns().stream()
                .map(colDesc -> new ColumnDescriptor(colDesc.type(), colDesc.nullable()))
                .collect(toUnmodifiableList());
    }

    /**
     * Creates a new instance for a Hash Index.
     */
    public BinaryTupleRowSerializer(HashIndexDescriptor descriptor) {
        this.schema = descriptor.indexColumns().stream()
                .map(colDesc -> new ColumnDescriptor(colDesc.type(), colDesc.nullable()))
                .collect(toUnmodifiableList());
    }

    /**
     * Creates an {@link IndexRow} from the given index columns and a Row ID.
     */
    public IndexRow serializeRow(Object[] columnValues, RowId rowId) {
        if (columnValues.length != schema.size()) {
            throw new IllegalArgumentException(String.format(
                    "Incorrect number of column values passed. Expected %d, got %d",
                    schema.size(),
                    columnValues.length
            ));
        }

        return new IndexRowImpl(serializeRowPrefix(columnValues), rowId);
    }

    /**
     * Creates a prefix of an {@link IndexRow} using the provided columns.
     */
    public BinaryTuple serializeRowPrefix(Object[] prefixColumnValues) {
        if (prefixColumnValues.length > schema.size()) {
            throw new IllegalArgumentException(String.format(
                    "Incorrect number of column values passed. Expected not more than %d, got %d",
                    schema.size(),
                    prefixColumnValues.length
            ));
        }

        Element[] prefixElements = schema.stream()
                .limit(prefixColumnValues.length)
                .map(columnDescriptor -> new Element(columnDescriptor.type, columnDescriptor.nullable))
                .toArray(Element[]::new);

        BinaryTupleSchema prefixSchema = BinaryTupleSchema.create(prefixElements);

        BinaryTupleBuilder builder = BinaryTupleBuilder.create(
                prefixSchema.elementCount(), prefixSchema.hasNullableElements());

        for (Object value : prefixColumnValues) {
            appendValue(builder, prefixSchema, value);
        }

        return new BinaryTuple(prefixSchema, builder.build());
    }

    /**
     * Converts a byte representation of index columns back into Java objects.
     */
    public Object[] deserializeColumns(IndexRow indexRow) {
        BinaryTuple tuple = indexRow.indexColumns();

        assert tuple.count() == schema.size();

        var result = new Object[schema.size()];

        for (int i = 0; i < result.length; i++) {
            NativeTypeSpec typeSpec = schema.get(i).type.spec();

            result[i] = typeSpec.objectValue(tuple, i);
        }

        return result;
    }

    /**
     * Append a value for the current element.
     *
     * @param builder Builder.
     * @param schema Tuple schema.
     * @param value Element value.
     * @return Builder for chaining.
     */
    private static BinaryTupleBuilder appendValue(BinaryTupleBuilder builder, BinaryTupleSchema schema, Object value) {
        Element element = schema.element(builder.elementIndex());

        if (value == null) {
            if (!element.nullable()) {
                throw new SchemaMismatchException("NULL value for non-nullable column in binary tuple builder.");
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
                return builder.appendDecimalNotNull((BigDecimal) value);
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
