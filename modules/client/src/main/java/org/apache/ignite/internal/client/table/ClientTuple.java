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

import org.apache.ignite.internal.binarytuple.BinaryTupleReader;
import org.apache.ignite.internal.client.proto.TuplePart;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.apache.ignite.sql.ColumnType;
import org.jetbrains.annotations.Nullable;

/**
 * Client tuple. Wraps {@link BinaryTupleReader} and allows mutability.
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
     * When false, only the columns corresponding to the part are present.
     */
    public ClientTuple(ClientSchema schema, TuplePart part, BinaryTupleReader binaryTuple) {
        super(binaryTuple, schema.columns(part).length, null);

        assert binaryTuple.elementCount() <= schema.columns().length
                : "Binary tuple element count is greater than schema column count: [binaryTuple="
                + binaryTuple.elementCount() + ", schema=" + schema.columns().length + ']';

        // TODO: This class does not work correctly with key-only tuples when keys are not in the beginning of the binaryTuple (e.g. RemoveAll).
        this.schema = schema;
        this.part = part;
        this.fullBinaryTuple = binaryTuple.elementCount() == schema.columns().length;
    }

    @Override
    protected String schemaColumnName(int index) {
        return schema.columns()[index].name();
    }

    @Override
    protected int internalIndex(String columnName) {
        return internalIndex(column(columnName));
    }

    @Override
    protected int internalIndex(int publicIndex) {
        if (part == TuplePart.KEY_AND_VAL) {
            return publicIndex;
        }

        return internalIndex(schema.columns(part)[publicIndex]);
    }

    private int internalIndex(@Nullable ClientColumn column) {
        // TODO: There is a mix up between publicIndex, internalIndex and schemaIndex!
        if (column == null) {
            return -1;
        }

        if (fullBinaryTuple) {
            return column.schemaIndex();
        }

        return part == TuplePart.KEY
                ? column.keyIndex()
                : column.valIndex();
    }


    @Override
    protected int publicIndex(int internalIndex) {
        if (part == TuplePart.KEY_AND_VAL) {
            return internalIndex;
        }

        var col = schema.columns()[internalIndex];

        return part == TuplePart.KEY ? col.keyIndex() : col.valIndex();
    }

    @Override
    protected ColumnType schemaColumnType(int internalIndex) {
        ClientColumn column = schema.columns()[internalIndex];

        return column.type();
    }

    @Override
    protected int schemaDecimalScale(int internalIndex) {
        return schema.columns()[internalIndex].scale();
    }

    @Nullable
    private ClientColumn column(String columnName) {
        return schema.columnSafe(IgniteNameUtils.parseSimpleName(columnName));
    }
}
