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
import org.jetbrains.annotations.Nullable;

/**
 * Realization of {@code InternalSqlRow} allowing to represent a SQL row with a single string column.
 */
public class InternalSqlRowSingleString implements InternalSqlRow {
    private final String val;
    private BinaryTuple row;

    /**
     * Constructor.
     *
     * @param string Value for single column row.
     */
    public InternalSqlRowSingleString(@Nullable String string) {
        this.val = string;
    }

    /** {@inheritDoc} */
    @Override
    @Nullable
    public String get(int idx) {
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
            row = new BinaryTuple(1, new BinaryTupleBuilder(1).appendString(val).build());
        }
        return row;
    }

}
