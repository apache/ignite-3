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

    /** Cached colocation hash value. */
    private int colocationHash;

    /**
     * Constructor.
     *
     * @param schema Schema.
     * @param row    Binary row representation.
     */
    public Row(SchemaDescriptor schema, BinaryRow row) {
        super(row.hasValue() ? schema.length() : schema.keyColumns().length(), row.tupleSlice());

        this.row = row;
        this.schema = schema;

        binaryTupleSchema = row.hasValue()
                ? BinaryTupleSchema.createRowSchema(schema)
                : BinaryTupleSchema.createKeySchema(schema);
    }

    /**
     * Get row schema.
     */
    @Override
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
    public int elementCount() {
        return schema.length();
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

            for (Column c : schema().colocationColumns()) {
                ColocationUtils.append(hashCalc, value(c.schemaIndex()), c.type());
            }

            colocationHash = h0 = hashCalc.hash();
        }

        return h0;
    }

    @Override
    public BinaryTuple binaryTuple() {
        return new BinaryTuple(binaryTupleSchema.elementCount(), row.tupleSlice());
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
