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
import org.apache.ignite.internal.lang.InternalTuple;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;

/**
 * A projected tuple that doesn't require delegate to be in particular format.
 *
 * <p>During normalization, the original tuple will be read field by field with regard to provided projection.
 * Such an approach had an additional overhead on (de-)serialization fields value, but had no requirement for the tuple
 * to be compatible with Binary Tuple format.
 *
 * <p>Not thread safe!
 *
 * @see AbstractProjectedTuple
 */
public class FieldDeserializingProjectedTuple extends AbstractProjectedTuple {
    private final BinaryTupleSchema schema;

    /**
     * Constructor.
     *
     * @param schema A schema of the original tuple (represented by delegate). Used to read content of the delegate to build a
     *         proper byte buffer which content satisfying the schema with regard to given projection.
     * @param delegate An original tuple to create projection from.
     * @param projection A projection. That is, desired order of fields in original tuple. In that projection, index of the array is
     *         an index of field in resulting projection, and an element of the array at that index is an index of column in original
     *         tuple.
     */
    public FieldDeserializingProjectedTuple(BinaryTupleSchema schema, InternalTuple delegate, int[] projection) {
        super(delegate, projection);

        this.schema = schema;
    }

    @Override
    protected void normalize() {
        var builder = new BinaryTupleBuilder(projection.length);
        var newProjection = new int[projection.length];

        for (int i = 0; i < projection.length; i++) {
            int col = projection[i];

            newProjection[i] = i;

            Element element = schema.element(col);

            BinaryRowConverter.appendValue(builder, element, schema.value(delegate, col));
        }

        delegate = new BinaryTuple(projection.length, builder.build());
        projection = newProjection;
    }
}
