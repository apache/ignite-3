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

package org.apache.ignite.internal.sql.engine.exec;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.row.InternalTuple;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.jetbrains.annotations.Nullable;

/**
 * TODO Sql binary row wrapper.
 */
public class SqlBinaryRowWrapper implements SqlRowWrapper {
    private final InternalTuple row;

    private final BinaryTupleSchema schema;

    private final List<Integer> requiredColumns;

    SqlBinaryRowWrapper(BinaryTupleSchema schema, ByteBuffer buf) {
        this.schema = schema;
        this.row = new BinaryTuple(schema.elementCount(), buf);
        this.requiredColumns = null;
    }

    SqlBinaryRowWrapper(BinaryTupleSchema schema, InternalTuple row, List<Integer> requiredColumns) {
        this.schema = schema;
        this.row = row;
        this.requiredColumns = requiredColumns;
    }

    @Override
    public int columnsCount() {
        return requiredColumns != null ? requiredColumns.size() : row.elementCount();
    }

    @Override
    public @Nullable Object get(int i) {
        int x = requiredColumns == null ? i : requiredColumns.get(i);

        return TypeUtils.toInternal(schema.value(row, x));
    }

    @Override
    public void set(int i, Object v) {
        // TODO
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer toByteBuffer() {
        return row.byteBuffer();
    }
}
