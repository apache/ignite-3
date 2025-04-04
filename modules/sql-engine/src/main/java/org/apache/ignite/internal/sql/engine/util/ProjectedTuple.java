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

import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.binarytuple.BinaryTupleParser.Sink;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.InternalTupleEx;

/**
 * A projected tuple wrapper that is best effort to avoiding unnecessary (de-)serialization during tuple normalization.
 *
 * <p>Not thread safe!
 *
 * @see AbstractProjectedTuple
 */
public class ProjectedTuple extends AbstractProjectedTuple {
    /**
     * Constructor.
     *
     * @param delegate An original tuple to create projection from.
     * @param projection A projection. That is, desired order of fields in original tuple. In that projection, index of the array is
     *         an index of field in resulting projection, and an element of the array at that index is an index of column in original
     *         tuple.
     */
    public ProjectedTuple(InternalTupleEx delegate, int[] projection) {
        super(delegate, projection);
    }

    @Override
    protected void normalize() {
        BinaryTupleBuilder builder;

        if (delegate instanceof BinaryTuple) {
            // Estimate total data size.
            var stats = new Sink() {
                int estimatedValueSize = 0;

                @Override
                public void nextElement(int index, int begin, int end) {
                    estimatedValueSize += end - begin;
                }
            };

            BinaryTuple binaryTuple = (BinaryTuple) delegate;

            for (int columnIndex : projection) {
                binaryTuple.fetch(columnIndex, stats);
            }

            builder = new BinaryTupleBuilder(projection.length, stats.estimatedValueSize, true);
        } else {
            builder = new BinaryTupleBuilder(projection.length, 32, false);
        }

        assert delegate instanceof InternalTupleEx;
        InternalTupleEx delegate0 = (InternalTupleEx) delegate;

        // Now compose the tuple.
        int[] newProjection = new int[projection.length];
        for (int i = 0; i < projection.length; i++) {
            delegate0.copyValue(builder, projection[i]);

            newProjection[i] = i;
        }

        delegate = new BinaryTuple(projection.length, builder.build());
        projection = newProjection;
    }
}
