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
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.schema.AssemblyException;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowImpl;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaMismatchException;
import org.apache.ignite.internal.type.DecimalNativeType;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.type.TemporalNativeType;
import org.apache.ignite.internal.util.TemporalTypeUtils;
import org.apache.ignite.sql.ColumnType;
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
    private final int schemaVersion;
    private final List<Column> columns;
    private final BinaryTupleBuilder builder;

    /** Current field index (the field is unset). */
    private int curCol;

    /**
     * Creates a builder.
     *
     * @param schema Schema descriptor.
     * @param totalValueSize Total estimated length of non-NULL values, -1 if not known.
     */
    public RowAssembler(SchemaDescriptor schema, int totalValueSize) {
        this(schema.version(), schema.columns(), totalValueSize);
    }

    /**
     * Creates a builder.
     *
     * @param schema Schema descriptor.
     * @param totalValueSize Total estimated length of non-NULL values, -1 if not known.
     * @param exactEstimate Whether the total size is exact estimate or approximate.
     */
    public RowAssembler(SchemaDescriptor schema, int totalValueSize, boolean exactEstimate) {
        this(schema.version(), schema.columns(), totalValueSize, exactEstimate);
    }

    /**
     * Create a builder.
     *
     * @param schemaVersion Version of the schema.
     * @param columns List of columns to serialize. Values must be appended in the same order.
     * @param totalValueSize Total estimated length of non-NULL values, -1 if not known.
     */
    public RowAssembler(int schemaVersion, List<Column> columns, int totalValueSize) {
        this(schemaVersion, columns, totalValueSize, true);
    }

    /**
     * Create a builder.
     *
     * @param schemaVersion Version of the schema.
     * @param columns List of columns to serialize. Values must be appended in the same order.
     * @param totalValueSize Total estimated length of non-NULL values, -1 if not known.
     * @param exactEstimate Whether the total size is exact estimate or approximate.
     */
    public RowAssembler(int schemaVersion, List<Column> columns, int totalValueSize, boolean exactEstimate) {
        this.schemaVersion = schemaVersion;
        this.columns = columns;

        builder = new BinaryTupleBuilder(columns.size(), totalValueSize, exactEstimate);
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

        NativeType columnType = columns.get(curCol).type();

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
            case BYTE_ARRAY: {
                return appendBytes((byte[]) val);
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
        if (!columns.get(curCol).nullable()) {
            String name = columns.get(curCol).name();
            throw new SchemaMismatchException(Column.nullConstraintViolationMessage(name));
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
        Column column = columns.get(curCol);

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
     * Appends BigDecimal value for the current column to the chunk.
     *
     * @param val Column value.
     * @return {@code this} for chaining.
     * @throws SchemaMismatchException If a value doesn't match the current column type.
     */
    public RowAssembler appendDecimalNotNull(BigDecimal val) throws SchemaMismatchException {
        checkType(ColumnType.DECIMAL);

        Column col = columns.get(curCol);

        DecimalNativeType type = (DecimalNativeType) col.type();

        BigDecimal scaled = val.setScale(type.scale(), RoundingMode.HALF_UP);
        if (scaled.precision() > type.precision()) {
            throw new SchemaMismatchException(Column.numericFieldOverflow(col.name()));
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
        checkType(ColumnType.STRING);

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
        checkType(ColumnType.BYTE_ARRAY);

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
        checkType(ColumnType.TIME);

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
        checkType(ColumnType.DATETIME);

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
        checkType(ColumnType.TIMESTAMP);

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
        NativeType type = columns.get(curCol).type();
        return TemporalTypeUtils.normalizeNanos(nanos, ((TemporalNativeType) type).precision());
    }

    /**
     * Builds serialized row. The row buffer contains a schema version at the start of the buffer if value was appended, or {@code 0}
     * if only the key columns were populated.
     *
     * @return Created {@link BinaryRow}.
     */
    public BinaryRow build() {
        if (curCol != 0 && columns.size() != curCol) {
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
    private void checkType(ColumnType type) {
        Column col = columns.get(curCol);

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

    private void shiftColumn() {
        curCol++;
    }
}
