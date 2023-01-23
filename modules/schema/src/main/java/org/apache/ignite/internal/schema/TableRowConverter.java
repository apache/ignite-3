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

package org.apache.ignite.internal.schema;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleParser;
import org.apache.ignite.internal.binarytuple.BinaryTupleParser.Sink;
import org.jetbrains.annotations.Nullable;

/**
 * Utility to convert {@link TableRow} to {@link BinaryTuple} with specified columns set.
 */
public class TableRowConverter {

    private final BinaryTupleSchema srcSchema;
    private final BinaryTupleSchema dstSchema;

    /**
     * Constructor.
     *
     * @param srcSchema Source tuple schema.
     * @param dstSchema Index schema.
     */
    public TableRowConverter(BinaryTupleSchema srcSchema, BinaryTupleSchema dstSchema) {
        this.srcSchema = srcSchema;
        this.dstSchema = dstSchema;
    }

    /**
     * Convert a table row to a binary tuple.
     *
     * @param tableRow Table row.
     * @return Binary tuple.
     */

    public @Nullable BinaryTuple toTuple(@Nullable TableRow tableRow) {
        assert srcSchema.convertible();

        if (tableRow == null) {
            return null;
        }

        ByteBuffer tupleBuffer = tableRow.tupleSlice();
        var parser = new BinaryTupleParser(srcSchema.elementCount(), tupleBuffer);

        // See if there are any NULL values and estimate total data size.
        var stats = new Sink() {
            boolean hasNulls = false;
            int estimatedValueSize = 0;

            @Override
            public void nextElement(int index, int begin, int end) {
                if (begin == 0) {
                    hasNulls = true;
                } else {
                    estimatedValueSize += end - begin;
                }
            }
        };

        for (int elementIndex = 0; elementIndex < dstSchema.elementCount(); elementIndex++) {
            int columnIndex = dstSchema.columnIndex(elementIndex);
            parser.fetch(columnIndex, stats);
        }

        // Now compose the tuple.
        BinaryTupleBuilder builder = new BinaryTupleBuilder(dstSchema.elementCount(), stats.hasNulls, stats.estimatedValueSize);

        for (int elementIndex = 0; elementIndex < dstSchema.elementCount(); elementIndex++) {
            int columnIndex = dstSchema.columnIndex(elementIndex);
            parser.fetch(columnIndex, (index, begin, end) -> {
                if (begin == 0) {
                    builder.appendNull();
                } else {
                    builder.appendElementBytes(tupleBuffer, begin, end - begin);
                }
            });
        }
        return new BinaryTuple(dstSchema, builder.build());
    }

    public static TableRow fromBinaryRow(BinaryRow row, BinaryConverter converter) {
        return TableRow.createFromTuple(row.schemaVersion(), converter.toTuple(row));
    }
}
