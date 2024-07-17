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

package org.apache.ignite.internal.sql.engine;

import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.schema.BinaryTuple;

/**
 * Implementation of {@code InternalSqlRow} allowing to represent a SQL row with a single long column.
 */
public class InternalSqlRowSingleLong implements InternalSqlRow {
    private final long val;
    private BinaryTuple row;

    /**
     * Constructor.
     *
     * @param val Value for single column row.
     */
    public InternalSqlRowSingleLong(long val) {
        this.val = val;
    }

    /** {@inheritDoc} */
    @Override
    public Object get(int idx) {
        assert idx == 0;
        return val;
    }

    /** {@inheritDoc} */
    @Override
    public int fieldCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override
    public BinaryTuple asBinaryTuple() {
        if (row == null) {
            row = new BinaryTuple(1, new BinaryTupleBuilder(1, estimateSize(val)).appendLong(val).build());
        }
        return row;
    }

    private static int estimateSize(long value) {
        if (value <= Byte.MAX_VALUE && value >= Byte.MIN_VALUE) {
            return Byte.BYTES;
        } else if (value <= Short.MAX_VALUE && value >= Short.MIN_VALUE) {
            return Short.BYTES;
        } else if (value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE) {
            return Integer.BYTES;
        }

        return Long.BYTES;
    }
}
