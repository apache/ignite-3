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

import it.unimi.dsi.fastutil.ints.IntSet;
import java.util.Objects;

/**
 * Represents a marshalled object: the marshalled representation with information about how it was marshalled
 * (including what descriptors were used when marshalling it).
 */
public class MarshalledObject {
    /** Marshalled object representation. */
    private final byte[] bytes;

    /** IDs of the descriptors that were used while marshalling the object. */
    private final IntSet usedDescriptorIds;

    /**
     * Creates a new {@link MarshalledObject}.
     *
     * @param bytes           marshalled representation bytes
     * @param usedDescriptorIds the descriptors that were used to marshal the object
     */
    public MarshalledObject(byte[] bytes, IntSet usedDescriptorIds) {
        Objects.requireNonNull(bytes, "bytes is null");
        Objects.requireNonNull(usedDescriptorIds, "usedDescriptorIds is null");

        this.bytes = bytes;
        this.usedDescriptorIds = usedDescriptorIds;
    }

    /**
     * Returns marshalled object representation.
     *
     * @return marshalled object representation
     */
    public byte[] bytes() {
        return bytes;
    }

    /**
     * Returns IDs of the descriptors that were used while marshalling the object.
     *
     * @return IDs of the descriptors that were used while marshalling the object
     */
    public IntSet usedDescriptorIds() {
        return usedDescriptorIds;
    }
}
