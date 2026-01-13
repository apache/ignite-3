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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.type.VarlenNativeType;
import org.apache.ignite.internal.util.TupleTypeCastUtils;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/**
 * Column descriptor which contains a column name, a type and a nullability flag.
 */
public class Column {
    /** Default "default value supplier". */
    private static final DefaultValueProvider NULL_SUPPLIER = DefaultValueProvider.constantProvider(null);

    private final int rowPosition;
    private final int keyPosition;
    private final int valuePosition;
    private final int colocationPosition;
    private final String name;
    private final NativeType type;
    private final boolean nullable;

    @IgniteToStringExclude
    private final DefaultValueProvider defaultValueProvider;

    /**
     * Constructor.
     *
     * @param name Column name.
     * @param type An instance of column data type.
     * @param nullable If {@code false}, null values will not be allowed for this column.
     */
    public Column(
            String name,
            NativeType type,
            boolean nullable
    ) {
        this(-1, -1, -1, -1, name, type, nullable, NULL_SUPPLIER);
    }

    /**
     * Constructor.
     *
     * @param name Column name.
     * @param type An instance of column data type.
     * @param nullable f {@code false}, null values will not be allowed for this column.
     * @param defaultValueProvider Default value supplier.
     */
    public Column(
            String name,
            NativeType type,
            boolean nullable,
            DefaultValueProvider defaultValueProvider
    ) {
        this(-1, -1, -1, -1, name, type, nullable, defaultValueProvider);
    }

    /**
     * Constructor.
     *
     * @param rowPosition Position of a column in a full row.
     * @param keyPosition Position of a column in a key tuple.
     * @param valuePosition Position of a column in a value tuple.
     * @param colocationPosition Position of a column in a colocation key.
     * @param name Column name.
     * @param type An instance of column data type.
     * @param nullable If {@code false}, null values will not be allowed for this column.
     * @param defaultValueProvider Default value supplier.
     */
    private Column(
            int rowPosition,
            int keyPosition,
            int valuePosition,
            int colocationPosition,
            String name,
            NativeType type,
            boolean nullable,
            DefaultValueProvider defaultValueProvider
    ) {
        this.rowPosition = rowPosition;
        this.keyPosition = keyPosition;
        this.valuePosition = valuePosition;
        this.colocationPosition = colocationPosition;
        this.name = name;
        this.type = type;
        this.nullable = nullable;
        this.defaultValueProvider = defaultValueProvider;
    }

    /**
     * Get absolute position of this column in full row.
     */
    public int positionInRow() {
        return rowPosition;
    }

    /**
     * Get absolute position of this column in the key, or -1 when not a part of the key.
     */
    public int positionInKey() {
        return keyPosition;
    }

    /**
     * Get absolute position of this column in the value, or -1 when not a part of the value.
     */
    public int positionInValue() {
        return valuePosition;
    }

    /**
     * Get absolute position of this column in the colocation key, or -1 when not a part of the key.
     */
    public int positionInColocation() {
        return colocationPosition;
    }

    /**
     * Get column name.
     */
    public String name() {
        return name;
    }

    /**
     * Get an instance of column data type.
     */
    public NativeType type() {
        return type;
    }

    /**
     * Get nullable flag: {@code false} if null values will not be allowed for this column.
     */
    public boolean nullable() {
        return nullable;
    }

    /**
     * Returns provider for a column's default.
     */
    public DefaultValueProvider defaultValueProvider() {
        return defaultValueProvider;
    }

    /**
     * Get default value for the column.
     *
     * @return Default value.
     */
    public Object defaultValue() {
        return defaultValueProvider.get();
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Column col = (Column) o;

        return name.equals(col.name) && type.equals(col.type);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return name.hashCode() + 31 * type.hashCode();
    }

    /**
     * Validate the object by column's constraint.
     *
     * @param val Object to validate.
     */
    public void validate(@Nullable Object val) {
        if (val == null) {
            if (nullable) {
                return;
            } else {
                throw new SchemaMismatchException(nullConstraintViolationMessage(name));
            }
        }

        // assert false;

        NativeType objType = NativeTypes.fromObject(val);

        if (objType != null && type.mismatch(objType)) {
            boolean specMatches = objType.spec() == type.spec();

            if (specMatches && type instanceof VarlenNativeType) {
                String error = format("Value too long [column='{}', type={}]", name, type.displayName());
                throw new InvalidTypeException(error);
            } else {
                if (TupleTypeCastUtils.isCastAllowed(objType.spec(), type.spec(), val)) {
                    return;
                }

                String error = format(
                        "Value type does not match [column='{}', expected={}, actual={}]",
                        name, type.displayName(), objType.displayName()
                );
                throw new InvalidTypeException(error);
            }
        }

        if (type.spec() == ColumnType.DATE) {
            checkBounds((LocalDate) val, SchemaUtils.DATE_MIN, SchemaUtils.DATE_MAX);
        } else if (type.spec() == ColumnType.DATETIME) {
            checkBounds((LocalDateTime) val, SchemaUtils.DATETIME_MIN, SchemaUtils.DATETIME_MAX);
        } else if (type.spec() == ColumnType.TIMESTAMP) {
            checkBounds((Instant) val, SchemaUtils.TIMESTAMP_MIN, SchemaUtils.TIMESTAMP_MAX);
        }
    }

    private <T extends Comparable<T>> void checkBounds(T value, T min, T max) {
        if (value.compareTo(min) < 0 || value.compareTo(max) > 0) {
            throw new ValueOutOfBoundsException(format("Value is out of allowed range"
                    + " (column='{}', value='{}', min='{}', max='{}').", name, value, min, max));
        }
    }

    /**
     * Creates copy of the column with assigned positions.
     *
     * @param rowPosition Position of this column in full row tuple.
     * @param keyPosition Position of this column in key tuple.
     *      -1 if this column doesn't belong to key.
     * @param valuePosition Position of this column in value tuple.
     *      -1 if this column doesn't belong to value.
     * @param colocationPosition Position of this column in key tuple.
     *      -1 if this column doesn't belong to colocation key.
     * @return Column.
     */
    Column copy(int rowPosition, int keyPosition, int valuePosition, int colocationPosition) {
        assert (keyPosition == -1 && valuePosition >= 0)
                || (keyPosition >= 0 && valuePosition == -1)
                : "keyPosition=" + keyPosition + ", valuePosition=" + valuePosition;

        return new Column(rowPosition, keyPosition, valuePosition, colocationPosition, name, type, nullable, defaultValueProvider);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(Column.class, this);
    }

    /**
     * Returns an error message for NOT NULL constraint violation.
     *
     * @param columnName Column name.
     * @return Error message.
     */
    public static String nullConstraintViolationMessage(String columnName) {
        return format("Column '{}' does not allow NULLs", columnName);
    }

    /**
     * Returns an error message for numeric field overflow error.
     *
     * @param columnName Column name.
     * @return Error message.
     */
    public static String numericFieldOverflow(String columnName) {
        return format("Numeric field overflow in column '{}'", columnName);
    }
}
