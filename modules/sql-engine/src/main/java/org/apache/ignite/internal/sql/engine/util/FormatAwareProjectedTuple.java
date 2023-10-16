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

package org.apache.ignite.internal.sql.engine.util;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleParser;
import org.apache.ignite.internal.binarytuple.BinaryTupleParser.Sink;
import org.apache.ignite.internal.lang.InternalTuple;
import org.apache.ignite.internal.schema.BinaryTuple;

/**
 * A projected tuple that aware of the format of delegate.
 *
 * <p>That is, the format of delegate is known to be Binary Tuple, thus it's possible to avoid unnecessary
 * (de-)serialization during tuple normalization.
 *
 * <p>It's up to the caller to get sure that provided tuple respect the format.
 *
 * <p>Not thread safe!
 *
 * @see AbstractProjectedTuple
 */
public class FormatAwareProjectedTuple extends AbstractProjectedTuple {
    /**
     * Constructor.
     *
     * @param delegate An original tuple to create projection from.
     * @param projection A projection. That is, desired order of fields in original tuple. In that projection, index of the array is
     *         an index of field in resulting projection, and an element of the array at that index is an index of column in original
     *         tuple.
     */
    public FormatAwareProjectedTuple(InternalTuple delegate, int[] projection) {
        super(delegate, projection);
    }

    @Override
    protected void normalize() {
        int[] newProjection = new int[projection.length];
        ByteBuffer tupleBuffer = delegate.byteBuffer();

        BinaryTupleParser parser = new BinaryTupleParser(delegate.elementCount(), tupleBuffer);

        // Estimate total data size.
        var stats = new Sink() {
            int estimatedValueSize = 0;

            @Override
            public void nextElement(int index, int begin, int end) {
                estimatedValueSize += end - begin;
            }
        };

        for (int columnIndex : projection) {
            parser.fetch(columnIndex, stats);
        }

        // Now compose the tuple.
        BinaryTupleBuilder builder = new BinaryTupleBuilder(projection.length, stats.estimatedValueSize);

        for (int i = 0; i < projection.length; i++) {
            int columnIndex = projection[i];

            parser.fetch(columnIndex, (index, begin, end) -> {
                if (begin == end) {
                    builder.appendNull();
                } else {
                    builder.appendElementBytes(tupleBuffer, begin, end - begin);
                }
            });

            newProjection[i] = columnIndex;
        }

        delegate = new BinaryTuple(projection.length, builder.build());
        projection = newProjection;
    }
}
