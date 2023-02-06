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

package org.apache.ignite.internal.schema.row;


import static org.apache.ignite.internal.schema.BinaryRow.TUPLE_OFFSET;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;

/**
 * Class for building binary tuples intended to use in the {@link Row}, in which additional fields are stored before the tuple contents.
 */
class RowBinaryTupleBuilder extends BinaryTupleBuilder {
    RowBinaryTupleBuilder(int numElements, boolean allowNulls, int totalValueSize) {
        super(numElements, allowNulls, totalValueSize);
    }

    @Override
    protected ByteBuffer allocateBuffer(int capacity) {
        // Allocate space for additional fields.
        return ByteBuffer.allocate(TUPLE_OFFSET + capacity).position(TUPLE_OFFSET).slice();
    }

    @Override
    protected ByteBuffer reallocateBuffer(int capacity) {
        // Allocate space for additional fields.
        ByteBuffer newBuffer = ByteBuffer.allocate(TUPLE_OFFSET + capacity).order(ByteOrder.LITTLE_ENDIAN);

        // Copy only the filled data from the old buffer mimicking the flip method.
        int position = buffer.position();
        newBuffer.put(buffer.array(), 0, TUPLE_OFFSET + position);

        // First create a slice to keep the array offset, then restore the position.
        return newBuffer.position(TUPLE_OFFSET).slice().order(ByteOrder.LITTLE_ENDIAN).position(position);
    }
}
