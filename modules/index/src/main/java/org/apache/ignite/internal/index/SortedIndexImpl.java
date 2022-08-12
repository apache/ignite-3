/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.index;

import java.util.BitSet;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.util.Cursor;

/**
 * An object that represents a sorted index.
 */
public class SortedIndexImpl implements SortedIndex {
    private final UUID id;
    private final UUID tableId;
    private final SortedIndexDescriptor descriptor;

    /**
     * Constructs the sorted index.
     *
     * @param id An identifier of the index.
     * @param tableId An identifier of the table this index relates to.
     * @param descriptor A descriptor of the index.
     */
    public SortedIndexImpl(UUID id, UUID tableId, SortedIndexDescriptor descriptor) {
        this.id = Objects.requireNonNull(id, "id");
        this.tableId = Objects.requireNonNull(tableId, "tableId");
        this.descriptor = Objects.requireNonNull(descriptor, "descriptor");
    }

    /** {@inheritDoc} */
    @Override
    public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override
    public UUID tableId() {
        return tableId;
    }

    /** {@inheritDoc} */
    @Override
    public String name() {
        return descriptor.name();
    }

    /** {@inheritDoc} */
    @Override
    public SortedIndexDescriptor descriptor() {
        return descriptor;
    }

    /** {@inheritDoc} */
    @Override
    public Cursor<BinaryTuple> scan(BinaryTuple key, BitSet columns) {
        throw new UnsupportedOperationException("Index scan is not implemented yet");
    }

    /** {@inheritDoc} */
    @Override
    public Cursor<BinaryTuple> scan(BinaryTuple left, BinaryTuple right, byte includeBounds, BitSet columns) {
        throw new UnsupportedOperationException("Index scan is not implemented yet");
    }
}
