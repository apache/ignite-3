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

package org.apache.ignite.internal.schema.registry;

import static org.apache.ignite.internal.catalog.commands.CatalogUtils.isSupportedColumnTypeChange;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaException;
import org.apache.ignite.internal.schema.mapping.ColumnMapper;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.type.NativeTypeSpec;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/**
 * Adapter for row of older schema.
 */
public class UpgradingRowAdapter implements Row {
    /** Column mapper. */
    private final ColumnMapper mapper;

    /** Adapter schema. */
    private final SchemaDescriptor newSchema;

    private final BinaryTupleSchema newBinaryTupleSchema;

    /** Row of previous version. */
    private final Row row;

    private UpgradingRowAdapter(SchemaDescriptor newSchema, BinaryTupleSchema newBinaryTupleSchema, Row row, ColumnMapper mapper) {
        this.row = row;

        this.newSchema = newSchema;
        this.mapper = mapper;
        this.newBinaryTupleSchema = newBinaryTupleSchema;
    }

    /**
     * Creates an adapter that converts a given {@code row} to a new schema.
     *
     * @param newSchema New schema that the {@code row} will be converted to.
     * @param mapper Column mapper for converting columns to the new schema.
     * @param row Row to convert.
     * @return Adapter that converts a given {@code row} to a new schema.
     */
    public static UpgradingRowAdapter upgradeRow(SchemaDescriptor newSchema, ColumnMapper mapper, Row row) {
        return new UpgradingRowAdapter(newSchema, BinaryTupleSchema.createRowSchema(newSchema), row, mapper);
    }

    /**
     * Creates an adapter that converts a given {@code row}, that only contains a key component, to a new schema.
     *
     * @param newSchema New schema that the {@code row} will be converted to.
     * @param mapper Column mapper for converting columns to the new schema.
     * @param row Row to convert, that only contains a key component.
     * @return Adapter that converts a given {@code row} to a new schema.
     */
    public static UpgradingRowAdapter upgradeKeyOnlyRow(SchemaDescriptor newSchema, ColumnMapper mapper, Row row) {
        return new UpgradingRowAdapter(newSchema, BinaryTupleSchema.createKeySchema(newSchema), row, mapper);
    }

    /** {@inheritDoc} */
    @Override
    public SchemaDescriptor schema() {
        return newSchema;
    }

    /** {@inheritDoc} */
    @Override
    public int schemaVersion() {
        return newSchema.version();
    }

    /** {@inheritDoc} */
    @Override
    public boolean keyOnly() {
        return false;
    }

    /**
     * Map column.
     *
     * @param colIdx Column index in source schema.
     * @return Column index in target schema.
     */
    private int mapColumn(int colIdx) throws InvalidTypeException {
        return mapper.map(colIdx);
    }

    /** {@inheritDoc} */
    @Override
    public Object value(int colIdx) {
        int mappedId = mapColumn(colIdx);

        return mappedId < 0
                ? mapper.mappedColumn(colIdx).defaultValue()
                : newBinaryTupleSchema.value(this, colIdx);
    }

    /** {@inheritDoc} */
    @Override
    public boolean booleanValue(int colIdx) {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : row.schema().column(mappedId);

        if (NativeTypeSpec.BOOLEAN != column.type().spec()) {
            throw new SchemaException("Type conversion is not supported yet.");
        }

        return mappedId < 0 ? (boolean) column.defaultValue() : row.booleanValue(mappedId);
    }

    /** {@inheritDoc} */
    @Override
    public Boolean booleanValueBoxed(int colIdx) {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : row.schema().column(mappedId);

        if (NativeTypeSpec.BOOLEAN != column.type().spec()) {
            throw new SchemaException("Type conversion is not supported yet.");
        }

        return mappedId < 0 ? (Boolean) column.defaultValue() : row.booleanValueBoxed(mappedId);
    }

    /** {@inheritDoc} */
    @Override
    public byte byteValue(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : row.schema().column(mappedId);

        ensureTypeConversionAllowed(column.type().spec().asColumnType(), ColumnType.INT8);

        return mappedId < 0 ? (byte) column.defaultValue() : row.byteValue(mappedId);
    }

    /** {@inheritDoc} */
    @Override
    public Byte byteValueBoxed(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : row.schema().column(mappedId);

        ensureTypeConversionAllowed(column.type().spec().asColumnType(), ColumnType.INT8);

        return mappedId < 0 ? (Byte) column.defaultValue() : row.byteValueBoxed(mappedId);
    }

    /** {@inheritDoc} */
    @Override
    public short shortValue(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : row.schema().column(mappedId);

        ensureTypeConversionAllowed(column.type().spec().asColumnType(), ColumnType.INT16);

        return mappedId < 0 ? (short) column.defaultValue() : row.shortValue(mappedId);
    }

    /** {@inheritDoc} */
    @Override
    public Short shortValueBoxed(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : row.schema().column(mappedId);

        ensureTypeConversionAllowed(column.type().spec().asColumnType(), ColumnType.INT16);

        return mappedId < 0 ? (Short) column.defaultValue() : row.shortValueBoxed(mappedId);
    }

    /** {@inheritDoc} */
    @Override
    public int intValue(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : row.schema().column(mappedId);

        ensureTypeConversionAllowed(column.type().spec().asColumnType(), ColumnType.INT32);

        return mappedId < 0 ? (int) column.defaultValue() : row.intValue(mappedId);
    }

    /** {@inheritDoc} */
    @Override
    public Integer intValueBoxed(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : row.schema().column(mappedId);

        ensureTypeConversionAllowed(column.type().spec().asColumnType(), ColumnType.INT32);

        return mappedId < 0 ? (Integer) column.defaultValue() : row.intValueBoxed(mappedId);
    }

    /** {@inheritDoc} */
    @Override
    public long longValue(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : row.schema().column(mappedId);

        ensureTypeConversionAllowed(column.type().spec().asColumnType(), ColumnType.INT64);

        return mappedId < 0 ? (long) column.defaultValue() : row.longValue(mappedId);
    }

    /** {@inheritDoc} */
    @Override
    public Long longValueBoxed(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : row.schema().column(mappedId);

        ensureTypeConversionAllowed(column.type().spec().asColumnType(), ColumnType.INT64);

        return mappedId < 0 ? (Long) column.defaultValue() : row.longValueBoxed(mappedId);
    }

    /** {@inheritDoc} */
    @Override
    public float floatValue(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : row.schema().column(mappedId);

        ensureTypeConversionAllowed(column.type().spec().asColumnType(), ColumnType.FLOAT);

        return mappedId < 0 ? (float) column.defaultValue() : row.floatValue(mappedId);
    }

    /** {@inheritDoc} */
    @Override
    public Float floatValueBoxed(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : row.schema().column(mappedId);

        ensureTypeConversionAllowed(column.type().spec().asColumnType(), ColumnType.FLOAT);

        return mappedId < 0 ? (Float) column.defaultValue() : row.floatValueBoxed(mappedId);
    }

    /** {@inheritDoc} */
    @Override
    public double doubleValue(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : row.schema().column(mappedId);

        ensureTypeConversionAllowed(column.type().spec().asColumnType(), ColumnType.DOUBLE);

        return mappedId < 0 ? (double) column.defaultValue() : row.doubleValue(mappedId);
    }


    /** {@inheritDoc} */
    @Override
    public Double doubleValueBoxed(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : row.schema().column(mappedId);

        ensureTypeConversionAllowed(column.type().spec().asColumnType(), ColumnType.DOUBLE);

        return mappedId < 0 ? (Double) column.defaultValue() : row.doubleValueBoxed(mappedId);
    }

    @Override
    public BigDecimal decimalValue(int colIdx) {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : row.schema().column(mappedId);

        if (NativeTypeSpec.DECIMAL != column.type().spec()) {
            throw new SchemaException("Type conversion is not supported yet.");
        }

        return mappedId < 0 ? (BigDecimal) column.defaultValue() : row.decimalValue(mappedId);
    }

    @Override
    public BigDecimal decimalValue(int colIdx, int scale) {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : row.schema().column(mappedId);

        if (NativeTypeSpec.DECIMAL != column.type().spec()) {
            throw new SchemaException("Type conversion is not supported yet.");
        }

        return mappedId < 0 ? (BigDecimal) column.defaultValue() : row.decimalValue(mappedId, scale);
    }

    /** {@inheritDoc} */
    @Override
    public BigInteger numberValue(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : row.schema().column(mappedId);

        if (NativeTypeSpec.NUMBER != column.type().spec()) {
            throw new SchemaException("Type conversion is not supported yet.");
        }

        return mappedId < 0 ? (BigInteger) column.defaultValue() : row.numberValue(mappedId);
    }

    /** {@inheritDoc} */
    @Override
    public String stringValue(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : row.schema().column(mappedId);

        if (NativeTypeSpec.STRING != column.type().spec()) {
            throw new SchemaException("Type conversion is not supported yet.");
        }

        return mappedId < 0 ? (String) column.defaultValue() : row.stringValue(mappedId);
    }

    /** {@inheritDoc} */
    @Override
    public byte[] bytesValue(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : row.schema().column(mappedId);

        if (NativeTypeSpec.BYTES != column.type().spec()) {
            throw new SchemaException("Type conversion is not supported yet.");
        }

        return mappedId < 0 ? (byte[]) column.defaultValue() : row.bytesValue(mappedId);
    }

    /** {@inheritDoc} */
    @Override
    public UUID uuidValue(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : row.schema().column(mappedId);

        if (NativeTypeSpec.UUID != column.type().spec()) {
            throw new SchemaException("Type conversion is not supported yet.");
        }

        return mappedId < 0 ? (UUID) column.defaultValue() : row.uuidValue(mappedId);
    }

    /** {@inheritDoc} */
    @Override
    public BitSet bitmaskValue(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : row.schema().column(mappedId);

        if (NativeTypeSpec.BITMASK != column.type().spec()) {
            throw new SchemaException("Type conversion is not supported yet.");
        }

        return mappedId < 0 ? (BitSet) column.defaultValue() : row.bitmaskValue(mappedId);
    }

    /** {@inheritDoc} */
    @Override
    public LocalDate dateValue(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : row.schema().column(mappedId);

        if (NativeTypeSpec.DATE != column.type().spec()) {
            throw new SchemaException("Type conversion is not supported yet.");
        }

        return mappedId < 0 ? (LocalDate) column.defaultValue() : row.dateValue(mappedId);
    }

    /** {@inheritDoc} */
    @Override
    public LocalTime timeValue(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : row.schema().column(mappedId);

        if (NativeTypeSpec.TIME != column.type().spec()) {
            throw new SchemaException("Type conversion is not supported yet.");
        }

        return mappedId < 0 ? (LocalTime) column.defaultValue() : row.timeValue(mappedId);
    }

    /** {@inheritDoc} */
    @Override
    public LocalDateTime dateTimeValue(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : row.schema().column(mappedId);

        if (NativeTypeSpec.DATETIME != column.type().spec()) {
            throw new SchemaException("Type conversion is not supported yet.");
        }

        return mappedId < 0 ? (LocalDateTime) column.defaultValue() : row.dateTimeValue(mappedId);
    }

    /** {@inheritDoc} */
    @Override
    public Instant timestampValue(int colIdx) throws InvalidTypeException {
        int mappedId = mapColumn(colIdx);

        Column column = mappedId < 0 ? mapper.mappedColumn(colIdx) : row.schema().column(mappedId);

        if (NativeTypeSpec.TIMESTAMP != column.type().spec()) {
            throw new SchemaException("Type conversion is not supported yet.");
        }

        return mappedId < 0 ? (Instant) column.defaultValue() : row.timestampValue(mappedId);
    }

    @Override
    public boolean hasNullValue(int colIdx) {
        int mappedId = mapColumn(colIdx);

        return mappedId < 0
                ? mapper.mappedColumn(colIdx).defaultValue() == null
                : row.hasNullValue(mappedId);
    }

    /** {@inheritDoc} */
    @Override
    public int colocationHash() {
        return row.colocationHash();
    }

    @Override
    public int elementCount() {
        return newBinaryTupleSchema.elementCount();
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable BinaryTuple binaryTuple() {
        // Underlying binary tuple can not be used directly.
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public ByteBuffer byteBuffer() {
        int size = newBinaryTupleSchema.elementCount();
        var builder = new BinaryTupleBuilder(size);

        for (int col = 0; col < size; col++) {
            Element element = newBinaryTupleSchema.element(col);

            BinaryRowConverter.appendValue(builder, element, value(col));
        }

        return new BinaryTuple(size, builder.build()).byteBuffer();
    }

    /** {@inheritDoc} */
    @Override
    public int tupleSliceLength() {
        throw new UnsupportedOperationException("Underlying binary can't be accessed directly.");
    }

    /** {@inheritDoc} */
    @Override
    public ByteBuffer tupleSlice() {
        throw new UnsupportedOperationException("Underlying binary can't be accessed directly.");
    }

    private void ensureTypeConversionAllowed(ColumnType from, ColumnType to) throws InvalidTypeException {
        if (!isSupportedColumnTypeChange(from, to)) {
            throw new SchemaException(format("Type conversion is not allowed: {} -> {}", from, to));
        }
    }
}
