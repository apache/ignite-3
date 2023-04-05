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
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleContainer;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.InvalidTypeException;
import org.apache.ignite.internal.schema.SchemaAware;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.util.ColocationUtils;
import org.apache.ignite.internal.util.HashCalculator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Schema-aware row.
 *
 * <p>The class contains non-generic methods to read boxed and unboxed primitives based on the schema column types. Any type conversions
 * and coercions should be implemented outside the row by the key-value or query runtime.
 *
 * <p>When a non-boxed primitive is read from a null column value, it is converted to the primitive type default value.
 */
public class Row implements BinaryRowEx, SchemaAware, InternalTuple, BinaryTupleContainer {

    /** Schema descriptor. */
    protected final SchemaDescriptor schema;

    /** Binary row. */
    private final BinaryRow row;

    /** Cached binary tuple extracted from the table row. */
    private final BinaryTuple tuple;

    /** Cached colocation hash value. */
    private int colocationHash;

    /**
     * Constructor.
     *
     * @param schema Schema.
     * @param row    Binary row representation.
     */
    public Row(SchemaDescriptor schema, BinaryRow row) {
        this.row = row;
        this.schema = schema;
        BinaryTupleSchema tupleSchema = row.hasValue()
                ? BinaryTupleSchema.createRowSchema(schema)
                : BinaryTupleSchema.createKeySchema(schema);
        tuple = new BinaryTuple(tupleSchema, row.tupleSlice());
    }

    /**
     * Get row schema.
     */
    @Override
    @NotNull
    public SchemaDescriptor schema() {
        return schema;
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasValue() {
        return row.hasValue();
    }

    /** {@inheritDoc} */
    @Override
    public int count() {
        return schema.length();
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     */
    public Object value(int col) {
        return schema.column(col).type().spec().objectValue(this, col);
    }

    /** {@inheritDoc} */
    @Override
    public byte byteValue(int col) throws InvalidTypeException {
        return tuple.byteValue(col);
    }

    /** {@inheritDoc} */
    @Override
    public Byte byteValueBoxed(int col) throws InvalidTypeException {
        return tuple.byteValueBoxed(col);
    }

    @Override
    public short shortValue(int index) {
        return tuple.shortValue(index);
    }

    @Override
    public Short shortValueBoxed(int index) {
        return tuple.shortValueBoxed(index);
    }

    @Override
    public int intValue(int index) {
        return tuple.intValue(index);
    }

    @Override
    public Integer intValueBoxed(int index) {
        return tuple.intValueBoxed(index);
    }

    @Override
    public long longValue(int index) {
        return tuple.longValue(index);
    }

    @Override
    public Long longValueBoxed(int index) {
        return tuple.longValueBoxed(index);
    }

    @Override
    public float floatValue(int index) {
        return tuple.floatValue(index);
    }

    @Override
    public Float floatValueBoxed(int index) {
        return tuple.floatValueBoxed(index);
    }

    @Override
    public double doubleValue(int index) {
        return tuple.doubleValue(index);
    }

    @Override
    public Double doubleValueBoxed(int index) {
        return tuple.doubleValueBoxed(index);
    }

    @Override
    public BigDecimal decimalValue(int index) {
        return tuple.decimalValue(index);
    }

    @Override
    public BigInteger numberValue(int index) {
        return tuple.numberValue(index);
    }

    @Override
    public String stringValue(int index) {
        return tuple.stringValue(index);
    }

    @Override
    public byte[] bytesValue(int index) {
        return tuple.bytesValue(index);
    }

    @Override
    public UUID uuidValue(int index) {
        return tuple.uuidValue(index);
    }

    @Override
    public BitSet bitmaskValue(int index) {
        return tuple.bitmaskValue(index);
    }

    @Override
    public LocalDate dateValue(int index) {
        return tuple.dateValue(index);
    }

    @Override
    public LocalTime timeValue(int index) {
        return tuple.timeValue(index);
    }

    @Override
    public LocalDateTime dateTimeValue(int index) {
        return tuple.dateTimeValue(index);
    }

    @Override
    public Instant timestampValue(int index) {
        return tuple.timestampValue(index);
    }

    @Override
    public boolean hasNullValue(int index) {
        return tuple.hasNullValue(index);
    }

    /** {@inheritDoc} */
    @Override
    public int schemaVersion() {
        return row.schemaVersion();
    }

    @Override
    public ByteBuffer tupleSlice() {
        return row.tupleSlice();
    }

    /** {@inheritDoc} */
    @Override
    public byte[] bytes() {
        return row.bytes();
    }

    @Override
    public ByteBuffer byteBuffer() {
        return row.byteBuffer();
    }

    /** {@inheritDoc} */
    @Override
    public int colocationHash() {
        int h0 = colocationHash;

        if (h0 == 0) {
            HashCalculator hashCalc = new HashCalculator();

            for (Column c : schema().colocationColumns()) {
                ColocationUtils.append(hashCalc, value(c.schemaIndex()), c.type());
            }

            colocationHash = h0 = hashCalc.hash();
        }

        return h0;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable BinaryTuple binaryTuple() {
        return tuple;
    }
}
