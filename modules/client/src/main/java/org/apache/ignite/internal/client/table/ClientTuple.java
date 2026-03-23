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

package org.apache.ignite.internal.client.table;

import java.util.Collection;
import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.client.proto.TuplePart;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.tx.Transaction;
import org.jetbrains.annotations.Nullable;

/**
 * Client tuple. Wraps {@link BinaryTupleReader} and allows mutability.
 *
 * <p>The following use cases are supported:
 * <ul>
 *     <li>Full binary tuple, TuplePart.KEY_AND_VAL - obvious one.</li>
 *     <li>Key only binary tuple, TuplePart.KEY - key only part,
 *     returned from methods like {@link org.apache.ignite.table.RecordView#deleteAll(Transaction, Collection)}.</li>
 *     <li>Full binary tuple, TuplePart.KEY - key part of full tuple, returned from KV APIs -
 *     single binary tuple is presented as a pair of {@link ClientTuple} instances.</li>
 *     <li>Full binary tuple, TuplePart.VAL - same as above, value part.</li>
 *     <li>Value only binary tuple, TuplePart.VAL - not used currently,
 *     but we might optimize the protocol to return value-only data for key-based operations.</li>
 * </ul>
 */
public class ClientTuple extends MutableTupleBinaryTupleAdapter {
    private final ClientSchema schema;

    private final TuplePart part;

    private final boolean fullBinaryTuple;

    /**
     * Constructor.
     *
     * @param schema Schema.
     * @param part Schema part.
     * @param binaryTuple Tuple.
     */
    public ClientTuple(ClientSchema schema, TuplePart part, BinaryTupleReader binaryTuple) {
        super(binaryTuple, schema.columns(part).length, null);

        assert binaryTuple.elementCount() <= schema.columns().length
                : "Binary tuple element count is greater than schema column count: [binaryTuple="
                + binaryTuple.elementCount() + ", schema=" + schema.columns().length + ']';

        this.schema = schema;
        this.part = part;
        this.fullBinaryTuple = binaryTuple.elementCount() == schema.columns().length;
    }

    @Override
    protected String schemaColumnName(int binaryTupleIndex) {
        return column(binaryTupleIndex).name();
    }

    @Override
    protected int binaryTupleIndex(String columnName) {
        return binaryTupleIndex(column(columnName));
    }

    @Override
    protected int binaryTupleIndex(int publicIndex) {
        if (part == TuplePart.KEY_AND_VAL) {
            return publicIndex;
        }

        return binaryTupleIndex(schema.columns(part)[publicIndex]);
    }

    private int binaryTupleIndex(@Nullable ClientColumn column) {
        if (column == null) {
            return -1;
        }

        if (fullBinaryTuple) {
            if (part == TuplePart.KEY && column.keyIndex() < 0) {
                return -1;
            }

            if (part == TuplePart.VAL && column.valIndex() < 0) {
                return -1;
            }

            return column.schemaIndex();
        }

        return part == TuplePart.KEY
                ? column.keyIndex()
                : column.valIndex();
    }

    @Override
    protected int publicIndex(int binaryTupleIndex) {
        if (part == TuplePart.KEY_AND_VAL) {
            return binaryTupleIndex;
        }

        var col = column(binaryTupleIndex);

        return part == TuplePart.KEY ? col.keyIndex() : col.valIndex();
    }

    @Override
    protected ColumnType schemaColumnType(int binaryTupleIndex) {
        ClientColumn column = column(binaryTupleIndex);

        return column.type();
    }

    @Override
    protected int schemaDecimalScale(int binaryTupleIndex) {
        return column(binaryTupleIndex).scale();
    }

    @Nullable
    private ClientColumn column(String columnName) {
        return schema.columnSafe(IgniteNameUtils.parseIdentifier(columnName));
    }

    private ClientColumn column(int binaryTupleIndex) {
        if (fullBinaryTuple) {
            return schema.columns()[binaryTupleIndex];
        }

        return schema.columns(part)[binaryTupleIndex];
    }
}
