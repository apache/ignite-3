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
 * Implementation of {@code InternalSqlRow} allowing to represent a SQL row with a single boolean column.
 */
public class InternalSqlRowSingleBoolean implements InternalSqlRow {
    private final boolean val;
    private BinaryTuple row;

    /**
     * Constructor.
     *
     * @param bool Value for single column row.
     */
    public InternalSqlRowSingleBoolean(boolean bool) {
        this.val = bool;
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
            row = new BinaryTuple(1, new BinaryTupleBuilder(1, 1).appendBoolean(val).build());
        }
        return row;
    }
}
