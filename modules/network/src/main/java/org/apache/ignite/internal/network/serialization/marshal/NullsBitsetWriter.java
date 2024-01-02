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

package org.apache.ignite.internal.network.serialization.marshal;

import java.io.DataOutput;
import java.io.IOException;
import java.util.BitSet;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.FieldDescriptor;
import org.jetbrains.annotations.Nullable;

/**
 * A component that writes nulls bitsets.
 * Only fields with non-primitive types known upfront {@link FieldDescriptor#isSerializationTypeKnownUpfront()}
 * are considered (for other fields, we do not write any bits).
 */
abstract class NullsBitsetWriter {
    /**
     * Writes a bitset of nulls (only fields with non-primitive types known upfront produce bits).
     * If the class does not have any fields producing such bits, then nothing is written and {@code null} is returned.
     *
     * @param object     object for which nulls bitset is written
     * @param descriptor object class descriptor
     * @param output     output to which to write the bitset
     * @return the bitset or {@code null} if bitset would contain no bits
     * @throws IOException if something goes wrong
     */
    @Nullable
    BitSet writeNullsBitSet(Object object, ClassDescriptor descriptor, DataOutput output) throws IOException {
        if (descriptor.fieldIndexInNullsBitmapSize() == 0) {
            return null;
        }

        BitSet nullsBitSet = new BitSet(descriptor.fieldIndexInNullsBitmapSize());

        for (FieldDescriptor fieldDescriptor : descriptor.fields()) {
            int indexInBitmap = descriptor.fieldIndexInNullsBitmap(fieldDescriptor.name());
            if (indexInBitmap >= 0) {
                Object fieldValue = getFieldValue(object, fieldDescriptor);
                if (fieldValue == null) {
                    nullsBitSet.set(indexInBitmap);
                }
            }
        }

        ProtocolMarshalling.writeFixedLengthBitSet(nullsBitSet, descriptor.fieldIndexInNullsBitmapSize(), output);

        return nullsBitSet;
    }

    /**
     * Obtains a field value from an object.
     *
     * @param target          object from which to get a field value
     * @param fieldDescriptor descriptor of the field which value needs to be gotten
     * @return field value
     */
    abstract Object getFieldValue(Object target, FieldDescriptor fieldDescriptor);
}
