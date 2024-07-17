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

package org.apache.ignite.internal.storage.index.impl;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.IndexRow;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.jetbrains.annotations.Nullable;

/**
 * Test-only implementation of a {@link HashIndexStorage}.
 */
public class TestHashIndexStorage extends AbstractTestIndexStorage implements HashIndexStorage {
    private final ConcurrentMap<ByteBuffer, NavigableSet<RowId>> index = new ConcurrentHashMap<>();

    private final StorageHashIndexDescriptor descriptor;

    /** Constructor. */
    public TestHashIndexStorage(int partitionId, StorageHashIndexDescriptor descriptor) {
        super(partitionId, descriptor);

        this.descriptor = descriptor;
    }

    @Override
    public StorageHashIndexDescriptor indexDescriptor() {
        return descriptor;
    }

    @Override
    Iterator<RowId> getRowIdIteratorForGetByBinaryTuple(BinaryTuple key) {
        return new RowIdIterator(key.byteBuffer());
    }

    @Override
    public void put(IndexRow row) {
        checkStorageClosed(false);

        index.compute(row.indexColumns().byteBuffer(), (k, v) -> {
            if (v == null) {
                v = new ConcurrentSkipListSet<>();
            }

            v.add(row.rowId());

            return v;
        });
    }

    @Override
    public void remove(IndexRow row) {
        checkStorageClosedOrInProcessOfRebalance(false);

        index.computeIfPresent(row.indexColumns().byteBuffer(), (k, v) -> {
            if (v.remove(row.rowId()) && v.isEmpty()) {
                return null;
            }

            return v;
        });
    }

    @Override
    void clear0() {
        index.clear();
    }

    /**
     * Returns all indexed row ids.
     */
    public Set<RowId> allRowsIds() {
        return index.values().stream().flatMap(Set::stream).collect(Collectors.toSet());
    }

    /**
     * Row IDs iterator that always returns up-to-date values.
     */
    private class RowIdIterator implements Iterator<RowId> {
        private final ByteBuffer key;

        @Nullable Boolean hasNext;

        @Nullable RowId rowId;

        RowIdIterator(ByteBuffer key) {
            this.key = key;
        }

        @Override
        public boolean hasNext() {
            if (hasNext != null) {
                return hasNext;
            }

            // Yes, we must read it every time, because concurrency.
            NavigableSet<RowId> rowIds = index.get(key);

            if (rowIds == null) {
                rowId = null;
            } else if (rowId == null) {
                rowId = rowIds.stream().findFirst().orElse(null);
            } else {
                rowId = rowIds.higher(rowId);
            }

            hasNext = rowId != null;

            return hasNext;
        }

        @Override
        public RowId next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            hasNext = null;

            return Objects.requireNonNull(rowId);
        }
    }
}
