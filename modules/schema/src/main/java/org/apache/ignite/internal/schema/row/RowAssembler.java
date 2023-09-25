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

package org.apache.ignite.internal.schema.row;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.schema.AssemblyException;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowImpl;
import org.apache.ignite.internal.schema.BitmaskNativeType;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.DecimalNativeType;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.NumberNativeType;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaMismatchException;
import org.apache.ignite.internal.schema.TemporalNativeType;
import org.apache.ignite.internal.util.TemporalTypeUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Utility class to build rows using column appending pattern. The external user of this class must consult with the schema and provide the
 * columns in strict internal column sort order during the row construction.
 *
 * <p>Additionally, the user of this class should pre-calculate the resulting row size when possible to avoid unnecessary data copies and
 * allow some size optimizations to be applied.
 *
 * <p>Natively supported temporal types are encoded automatically with preserving sort order before writing.
 */
public class RowAssembler {
    /** Key columns. */
    private final Columns keyColumns;

    /** Value columns. */
    private final Columns valueColumns;

    /** Schema version. */
    private final int schemaVersion;

    /** Binary tuple builder. */
    private final BinaryTupleBuilder builder;

    /** Current columns chunk. */
    private Columns curCols;

    /** Current field index (the field is unset). */
    private int curCol;

    /**
     * Creates a builder.
     *
     * @param schema Schema descriptor.
     */
    public RowAssembler(SchemaDescriptor schema) {
        this(schema, -1);
    }

    /**
     * Creates a builder.
     *
     * @param schema Schema descriptor.
     * @param totalValueSize Total estimated length of non-NULL values, -1 if not known.
     */
    public RowAssembler(SchemaDescriptor schema, int totalValueSize) {
        this(schema.keyColumns(), schema.valueColumns(), schema.version(), totalValueSize);
    }

    /**
     * Creates a builder.
     *
     * @param keyColumns Key columns.
     * @param valueColumns Value columns, {@code null} if only key should be assembled.
     * @param schemaVersion Schema version.
     * @param totalValueSize Total estimated length of non-NULL values, -1 if not known.
     */
    public RowAssembler(Columns keyColumns, @Nullable Columns valueColumns, int schemaVersion, int totalValueSize) {
        this.keyColumns = keyColumns;
        this.valueColumns = valueColumns;
        this.schemaVersion = schemaVersion;
        int numElements = keyColumns.length() + (valueColumns != null ? valueColumns.length() : 0);
        builder = new BinaryTupleBuilder(numElements, totalValueSize);
        curCols = keyColumns;
        curCol = 0;
    }

    /**
     * Helper method.
     *
     * @param val    Value.
     * @throws SchemaMismatchException If a value doesn't match the current column type.
     */
    public RowAssembler appendValue(@Nullable Object val) throws SchemaMismatchException {
        if (val == null) {
            return appendNull();
        }

        NativeType columnType = curCols.column(curCol).type();

        switch (columnType.spec()) {
            case BOOLEAN: {
                return appendBoolean((boolean) val);
            }
            case INT8: {
                return appendByte((byte) val);
            }
            case INT16: {
                return appendShort((short) val);
            }
            case INT32: {
                return appendInt((int) val);
            }
            case INT64: {
                return appendLong((long) val);
            }
            case FLOAT: {
                return appendFloat((float) val);
            }
            case DOUBLE: {
                return appendDouble((double) val);
            }
            case UUID: {
                return appendUuid((UUID) val);
            }
            case TIME: {
                return appendTime((LocalTime) val);
            }
            case DATE: {
                return appendDate((LocalDate) val);
            }
            case DATETIME: {
                return appendDateTime((LocalDateTime) val);
            }
            case TIMESTAMP: {
                return appendTimestamp((Instant) val);
            }
            case STRING: {
                return appendString((String) val);
            }
            case BYTES: {
                return appendBytes((byte[]) val);
            }
            case BITMASK: {
                return appendBitmask((BitSet) val);
            }
            case NUMBER: {
                return appendNumber((BigInteger) val);
            }
            case DECIMAL: {
                return appendDecimal((BigDecimal) val);
            }
            default:
                throw new InvalidTypeException("Unexpected value: " + columnType);
        }
    }

    /**
     * Appends {@code null} value for the current column to the chunk.
     *
     * @return {@code this} for chaining.
     * @throws SchemaMismatchException If the current column is not nullable.
     */
    public RowAssembler appendNull() throws SchemaMismatchException {
        if (!curCols.column(curCol).nullable()) {
            throw new SchemaMismatchException(
                    "Failed to set column (null was passed, but column is not nullable): " + curCols.column(curCol));
        }

        builder.appendNull();

        shiftColumn();

        return this;
    }

    /**
     * Appends the default value for the current column.
     *
     * @return {@code this} for chaining.
     */
    public RowAssembler appendDefault() {
        Column column = curCols.column(curCol);

        return appendValue(column.defaultValue());
    }

    /**
     * Appends boolean value for the current column to the chunk.
     *
     * @param val Column value.
     * @return {@code this} for chaining.
     * @throws SchemaMismatchException If a value doesn't match the current column type.
     */
    public RowAssembler appendBoolean(boolean val) throws SchemaMismatchException {
        checkType(NativeTypes.BOOLEAN);

        builder.appendBoolean(val);

        shiftColumn();

        return this;
    }

    public RowAssembler appendBoolean(Boolean value) throws SchemaMismatchException {
        return value == null ? appendNull() : appendBoolean(value.booleanValue());
    }

    /**
     * Appends byte value for the current column to the chunk.
     *
     * @param val Column value.
     * @return {@code this} for chaining.
     * @throws SchemaMismatchException If a value doesn't match the current column type.
     */
    public RowAssembler appendByte(byte val) throws SchemaMismatchException {
        checkType(NativeTypes.INT8);

        builder.appendByte(val);

        shiftColumn();

        return this;
    }

    public RowAssembler appendByte(Byte value) throws SchemaMismatchException {
        return value == null ? appendNull() : appendByte(value.byteValue());
    }

    /**
     * Appends short value for the current column to the chunk.
     *
     * @param val Column value.
     * @return {@code this} for chaining.
     * @throws SchemaMismatchException If a value doesn't match the current column type.
     */
    public RowAssembler appendShort(short val) throws SchemaMismatchException {
        checkType(NativeTypes.INT16);

        builder.appendShort(val);

        shiftColumn();

        return this;
    }

    public RowAssembler appendShort(Short value) throws SchemaMismatchException {
        return value == null ? appendNull() : appendShort(value.shortValue());
    }

    /**
     * Appends int value for the current column to the chunk.
     *
     * @param val Column value.
     * @return {@code this} for chaining.
     * @throws SchemaMismatchException If a value doesn't match the current column type.
     */
    public RowAssembler appendInt(int val) throws SchemaMismatchException {
        checkType(NativeTypes.INT32);

        builder.appendInt(val);

        shiftColumn();

        return this;
    }

    public RowAssembler appendInt(@Nullable Integer value) {
        return value == null ? appendNull() : appendInt(value.intValue());
    }

    /**
     * Appends long value for the current column to the chunk.
     *
     * @param val Column value.
     * @return {@code this} for chaining.
     * @throws SchemaMismatchException If a value doesn't match the current column type.
     */
    public RowAssembler appendLong(long val) throws SchemaMismatchException {
        checkType(NativeTypes.INT64);

        builder.appendLong(val);

        shiftColumn();

        return this;
    }

    public RowAssembler appendLong(Long value) {
        return value == null ? appendNull() : appendLong(value.longValue());
    }

    /**
     * Appends float value for the current column to the chunk.
     *
     * @param val Column value.
     * @return {@code this} for chaining.
     * @throws SchemaMismatchException If a value doesn't match the current column type.
     */
    public RowAssembler appendFloat(float val) throws SchemaMismatchException {
        checkType(NativeTypes.FLOAT);

        builder.appendFloat(val);

        shiftColumn();

        return this;
    }

    public RowAssembler appendFloat(Float value) {
        return value == null ? appendNull() : appendFloat(value.floatValue());
    }

    /**
     * Appends double value for the current column to the chunk.
     *
     * @param val Column value.
     * @return {@code this} for chaining.
     * @throws SchemaMismatchException If a value doesn't match the current column type.
     */
    public RowAssembler appendDouble(double val) throws SchemaMismatchException {
        checkType(NativeTypes.DOUBLE);

        builder.appendDouble(val);

        shiftColumn();

        return this;
    }

    public RowAssembler appendDouble(Double value) {
        return value == null ? appendNull() : appendDouble(value.doubleValue());
    }

    /**
     * Appends BigInteger value for the current column to the chunk.
     *
     * @param val Column value.
     * @return {@code this} for chaining.
     * @throws SchemaMismatchException If a value doesn't match the current column type.
     */
    public RowAssembler appendNumberNotNull(BigInteger val) throws SchemaMismatchException {
        checkType(NativeTypeSpec.NUMBER);

        Column col = curCols.column(curCol);

        NumberNativeType type = (NumberNativeType) col.type();

        //0 is a magic number for "unlimited precision"
        if (type.precision() > 0 && new BigDecimal(val).precision() > type.precision()) {
            throw new SchemaMismatchException("Failed to set number value for column '" + col.name() + "' "
                    + "(max precision exceeds allocated precision) "
                    + "[number=" + val + ", max precision=" + type.precision() + "]");
        }

        builder.appendNumberNotNull(val);

        shiftColumn();

        return this;
    }

    public RowAssembler appendNumber(BigInteger val) throws SchemaMismatchException {
        return val == null ? appendNull() : appendNumberNotNull(val);
    }

    /**
     * Appends BigDecimal value for the current column to the chunk.
     *
     * @param val Column value.
     * @return {@code this} for chaining.
     * @throws SchemaMismatchException If a value doesn't match the current column type.
     */
    public RowAssembler appendDecimalNotNull(BigDecimal val) throws SchemaMismatchException {
        checkType(NativeTypeSpec.DECIMAL);

        Column col = curCols.column(curCol);

        DecimalNativeType type = (DecimalNativeType) col.type();

        if (val.setScale(type.scale(), RoundingMode.HALF_UP).precision() > type.precision()) {
            throw new SchemaMismatchException("Failed to set decimal value for column '" + col.name() + "' "
                    + "(max precision exceeds allocated precision)"
                    + " [decimal=" + val + ", max precision=" + type.precision() + "]");
        }

        builder.appendDecimalNotNull(val, type.scale());

        shiftColumn();

        return this;
    }

    public RowAssembler appendDecimal(BigDecimal value) {
        return value == null ? appendNull() : appendDecimalNotNull(value);
    }

    /**
     * Appends String value for the current column to the chunk.
     *
     * @param val Column value.
     * @return {@code this} for chaining.
     * @throws SchemaMismatchException If a value doesn't match the current column type.
     */
    public RowAssembler appendStringNotNull(String val) throws SchemaMismatchException {
        checkType(NativeTypeSpec.STRING);

        builder.appendStringNotNull(val);

        shiftColumn();

        return this;
    }

    public RowAssembler appendString(@Nullable String value) {
        return value == null ? appendNull() : appendStringNotNull(value);
    }

    /**
     * Appends byte[] value for the current column to the chunk.
     *
     * @param val Column value.
     * @return {@code this} for chaining.
     * @throws SchemaMismatchException If a value doesn't match the current column type.
     */
    public RowAssembler appendBytesNotNull(byte[] val) throws SchemaMismatchException {
        checkType(NativeTypeSpec.BYTES);

        builder.appendBytesNotNull(val);

        shiftColumn();

        return this;
    }

    public RowAssembler appendBytes(byte[] value) {
        return value == null ? appendNull() : appendBytesNotNull(value);
    }

    /**
     * Appends UUID value for the current column to the chunk.
     *
     * @param uuid Column value.
     * @return {@code this} for chaining.
     * @throws SchemaMismatchException If a value doesn't match the current column type.
     */
    public RowAssembler appendUuidNotNull(UUID uuid) throws SchemaMismatchException {
        checkType(NativeTypes.UUID);

        builder.appendUuidNotNull(uuid);

        shiftColumn();

        return this;
    }

    public RowAssembler appendUuid(UUID value) {
        return value == null ? appendNull() : appendUuidNotNull(value);
    }

    /**
     * Appends BitSet value for the current column to the chunk.
     *
     * @param bitSet Column value.
     * @return {@code this} for chaining.
     * @throws SchemaMismatchException If a value doesn't match the current column type.
     */
    public RowAssembler appendBitmaskNotNull(BitSet bitSet) throws SchemaMismatchException {
        Column col = curCols.column(curCol);

        checkType(NativeTypeSpec.BITMASK);

        BitmaskNativeType maskType = (BitmaskNativeType) col.type();

        if (bitSet.length() > maskType.bits()) {
            throw new IllegalArgumentException("Failed to set bitmask for column '" + col.name() + "' "
                    + "(mask size exceeds allocated size) [mask=" + bitSet + ", maxSize=" + maskType.bits() + "]");
        }

        builder.appendBitmaskNotNull(bitSet);

        shiftColumn();

        return this;
    }

    public RowAssembler appendBitmask(BitSet value) {
        return value == null ? appendNull() : appendBitmaskNotNull(value);
    }

    /**
     * Appends LocalDate value for the current column to the chunk.
     *
     * @param val Column value.
     * @return {@code this} for chaining.
     * @throws SchemaMismatchException If a value doesn't match the current column type.
     */
    public RowAssembler appendDateNotNull(LocalDate val) throws SchemaMismatchException {
        checkType(NativeTypes.DATE);

        builder.appendDateNotNull(val);

        shiftColumn();

        return this;
    }

    public RowAssembler appendDate(LocalDate value) {
        return value == null ? appendNull() : appendDateNotNull(value);
    }

    /**
     * Appends LocalTime value for the current column to the chunk.
     *
     * @param val Column value.
     * @return {@code this} for chaining.
     * @throws SchemaMismatchException If a value doesn't match the current column type.
     */
    public RowAssembler appendTimeNotNull(LocalTime val) throws SchemaMismatchException {
        checkType(NativeTypeSpec.TIME);

        builder.appendTimeNotNull(normalizeTime(val));

        shiftColumn();

        return this;
    }

    public RowAssembler appendTime(LocalTime value) {
        return value == null ? appendNull() : appendTimeNotNull(value);
    }

    /**
     * Appends LocalDateTime value for the current column to the chunk.
     *
     * @param val Column value.
     * @return {@code this} for chaining.
     * @throws SchemaMismatchException If a value doesn't match the current column type.
     */
    public RowAssembler appendDateTimeNotNull(LocalDateTime val) throws SchemaMismatchException {
        checkType(NativeTypeSpec.DATETIME);

        builder.appendDateTimeNotNull(LocalDateTime.of(val.toLocalDate(), normalizeTime(val.toLocalTime())));

        shiftColumn();

        return this;
    }

    public RowAssembler appendDateTime(LocalDateTime value) {
        return value == null ? appendNull() : appendDateTimeNotNull(value);
    }

    /**
     * Appends Instant value for the current column to the chunk.
     *
     * @param val Column value.
     * @return {@code this} for chaining.
     * @throws SchemaMismatchException If a value doesn't match the current column type.
     */
    public RowAssembler appendTimestampNotNull(Instant val) throws SchemaMismatchException {
        checkType(NativeTypeSpec.TIMESTAMP);

        builder.appendTimestampNotNull(Instant.ofEpochSecond(val.getEpochSecond(), normalizeNanos(val.getNano())));

        shiftColumn();

        return this;
    }

    public RowAssembler appendTimestamp(Instant value) {
        return value == null ? appendNull() : appendTimestampNotNull(value);
    }

    private LocalTime normalizeTime(LocalTime val) {
        int nanos = normalizeNanos(val.getNano());
        return LocalTime.of(val.getHour(), val.getMinute(), val.getSecond(), nanos);
    }

    private int normalizeNanos(int nanos) {
        NativeType type = curCols.column(curCol).type();
        return TemporalTypeUtils.normalizeNanos(nanos, ((TemporalNativeType) type).precision());
    }

    /**
     * Builds serialized row. The row buffer contains a schema version at the start of the buffer if value was appended, or {@code 0}
     * if only the key columns were populated.
     *
     * @return Created {@link BinaryRow}.
     */
    public BinaryRow build() {
        if (keyColumns == curCols) {
            throw new AssemblyException("Key column missed: colIdx=" + curCol);
        } else if (curCol != 0 && valueColumns.length() != curCol) {
            throw new AssemblyException("Value column missed: colIdx=" + curCol);
        }

        return new BinaryRowImpl(schemaVersion, builder.build());
    }

    /**
     * Checks that the type being appended matches the column type.
     *
     * @param type Type spec that is attempted to be appended.
     * @throws SchemaMismatchException If given type doesn't match the current column type.
     */
    private void checkType(NativeTypeSpec type) {
        if (curCols == null) {
            throw new SchemaMismatchException("Failed to set column, expected key only but tried to add " + type.name());
        }

        Column col = curCols.column(curCol);

        // Column#validate does not work here, because we must tolerate differences in precision, size, etc.
        if (col.type().spec() != type) {
            throw new InvalidTypeException("Column's type mismatch ["
                    + "column=" + col
                    + ", expectedType=" + col.type().spec()
                    + ", actualType=" + type + ']');
        }
    }

    /**
     * Checks that the type being appended matches the column type.
     *
     * @param type Type that is attempted to be appended.
     */
    private void checkType(NativeType type) {
        checkType(type.spec());
    }

    /**
     * Shifts current column indexes as necessary, also switch to value chunk writer when moving from key to value columns.
     */
    private void shiftColumn() {
        curCol++;

        if (curCol == curCols.length() && curCols == keyColumns) {
            // Switch key->value columns.
            curCols = valueColumns;
            curCol = 0;
        }
    }

    /**
     * Creates an assembler which allows only key to be added.
     *
     * @param schema Schema descriptor.
     * @return Created assembler.
     */
    public static RowAssembler keyAssembler(SchemaDescriptor schema) {
        return new RowAssembler(schema.keyColumns(), null, schema.version(), -1);
    }
}
