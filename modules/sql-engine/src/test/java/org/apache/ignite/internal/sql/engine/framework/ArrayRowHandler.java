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

package org.apache.ignite.internal.sql.engine.framework;

import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.ignite.internal.schema.row.InternalTuple;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.ByteUtils;

/**
 * Handler for rows that implemented as a simple objects array.
 */
public class ArrayRowHandler implements RowHandler<Object[]> {
    public static final RowHandler<Object[]> INSTANCE = new ArrayRowHandler();

    private ArrayRowHandler() {
    }

    /** {@inheritDoc} */
    @Override
    public Object get(int field, Object[] row) {
        return row[field];
    }

    /** {@inheritDoc} */
    @Override
    public void set(int field, Object[] row, Object val) {
        row[field] = val;
    }

    /** {@inheritDoc} */
    @Override
    public Object[] concat(Object[] left, Object[] right) {
        return ArrayUtils.concat(left, right);
    }

    /** {@inheritDoc} */
    @Override
    public Object[] map(Object[] row, int[] mapping) {
        Object[] newRow = new Object[mapping.length];

        for (int i = 0; i < mapping.length; i++) {
            newRow[i] = row[mapping[i]];
        }

        return newRow;
    }

    /** {@inheritDoc} */
    @Override
    public int columnCount(Object[] row) {
        return row.length;
    }

    @Override
    public ByteBuffer toByteBuffer(Object[] row) {
        byte[] raw = ByteUtils.toBytes(row);
        return ByteBuffer.wrap(raw);
    }

    /** {@inheritDoc} */
    @Override
    public String toString(Object[] objects) {
        return "Row" + Arrays.toString(objects);
    }

    /** {@inheritDoc} */
    @Override
    public RowFactory<Object[]> factory(RowSchema rowSchema) {
        int rowLen = rowSchema.fields().size();

        return new RowFactory<>() {
            /** {@inheritDoc} */
            @Override
            public RowHandler<Object[]> handler() {
                return ArrayRowHandler.this;
            }

            /** {@inheritDoc} */
            @Override
            public Object[] create() {
                return new Object[rowLen];
            }

            /** {@inheritDoc} */
            @Override
            public Object[] create(Object... fields) {
                assert fields.length == rowLen;

                return fields;
            }

            /** {@inheritDoc} */
            @Override
            public Object[] create(ByteBuffer raw) {
                return ByteUtils.fromBytes(raw.array());
            }

            /** {@inheritDoc} */
            @Override
            public Object[] create(InternalTuple tuple) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
