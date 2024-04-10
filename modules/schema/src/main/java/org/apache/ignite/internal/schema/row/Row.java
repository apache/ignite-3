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
import java.nio.ByteBuffer;
import org.apache.ignite.internal.binarytuple.BinaryTupleContainer;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.lang.InternalTuple;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaAware;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.util.ColocationUtils;
import org.apache.ignite.internal.util.HashCalculator;

/**
 * Schema-aware row.
 *
 * <p>The class contains non-generic methods to read boxed and unboxed primitives based on the schema column types. Any type conversions
 * and coercions should be implemented outside the row by the key-value or query runtime.
 *
 * <p>When a non-boxed primitive is read from a null column value, it is converted to the primitive type default value.
 */
public class Row extends BinaryTupleReader implements BinaryRowEx, SchemaAware, InternalTuple, BinaryTupleContainer {
    /** Schema descriptor. */
    private final SchemaDescriptor schema;

    /** Binary row. */
    private final BinaryRow row;

    private final BinaryTupleSchema binaryTupleSchema;

    private final boolean keyOnly;

    /** Cached colocation hash value. */
    private int colocationHash;

    protected Row(boolean keyOnly, SchemaDescriptor schema, BinaryTupleSchema binaryTupleSchema, BinaryRow row) {
        super(binaryTupleSchema.elementCount(), row.tupleSlice());

        this.keyOnly = keyOnly;
        this.row = row;
        this.schema = schema;
        this.binaryTupleSchema = binaryTupleSchema;
    }

    /**
     * Creates a row from a given {@code BinaryRow}.
     *
     * @param schema Schema.
     * @param binaryRow Binary row.
     */
    public static Row wrapBinaryRow(SchemaDescriptor schema, BinaryRow binaryRow) {
        return new Row(false, schema, BinaryTupleSchema.createRowSchema(schema), binaryRow);
    }

    /**
     * Creates a row from a given {@code BinaryRow} that only contains the key component.
     *
     * @param schema Schema.
     * @param binaryRow Binary row.
     */
    public static Row wrapKeyOnlyBinaryRow(SchemaDescriptor schema, BinaryRow binaryRow) {
        return new Row(true, schema, BinaryTupleSchema.createKeySchema(schema), binaryRow);
    }

    /**
     * Get row schema.
     */
    @Override
    public SchemaDescriptor schema() {
        return schema;
    }

    /**
     * Gets a value indicating whether the row contains only key columns.
     *
     * @return {@code true} if the row contains only key columns.
     */
    public boolean keyOnly() {
        return keyOnly;
    }

    /**
     * Reads value for specified column.
     *
     * @param col Column index.
     * @return Column value.
     */
    public Object value(int col) {
        return binaryTupleSchema.value(this, col);
    }

    public BigDecimal decimalValue(int col) {
        return binaryTupleSchema.decimalValue(this, col);
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

    @Override
    public int tupleSliceLength() {
        return row.tupleSliceLength();
    }

    /** {@inheritDoc} */
    @Override
    public int colocationHash() {
        int h0 = colocationHash;

        if (h0 == 0) {
            HashCalculator hashCalc = new HashCalculator();

            for (Column c : schema.colocationColumns()) {
                int idx = keyOnly
                        ? c.positionInKey()
                        : c.positionInRow();

                assert idx >= 0 : c;

                ColocationUtils.append(hashCalc, value(idx), c.type());
            }

            colocationHash = h0 = hashCalc.hash();
        }

        return h0;
    }

    @Override
    public BinaryTuple binaryTuple() {
        return new BinaryTuple(binaryTupleSchema.elementCount(), row.tupleSlice());
    }

    public BinaryTupleSchema binaryTupleSchema() {
        return binaryTupleSchema;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Row row1 = (Row) o;

        return row.equals(row1.row);
    }

    @Override
    public int hashCode() {
        return row.hashCode();
    }
}
