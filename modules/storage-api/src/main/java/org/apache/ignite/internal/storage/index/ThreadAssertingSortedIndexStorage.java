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

package org.apache.ignite.internal.storage.index;

import static org.apache.ignite.internal.worker.ThreadAssertions.assertThreadAllowsToRead;

import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.worker.ThreadAssertingCursor;
import org.apache.ignite.internal.worker.ThreadAssertions;
import org.jetbrains.annotations.Nullable;

/**
 * {@link SortedIndexStorage} that performs thread assertions when doing read/write operations.
 *
 * @see ThreadAssertions
 */
public class ThreadAssertingSortedIndexStorage extends ThreadAssertingIndexStorage implements SortedIndexStorage {
    private final SortedIndexStorage indexStorage;

    /** Constructor. */
    public ThreadAssertingSortedIndexStorage(SortedIndexStorage indexStorage) {
        super(indexStorage);

        this.indexStorage = indexStorage;
    }

    @Override
    public StorageSortedIndexDescriptor indexDescriptor() {
        return indexStorage.indexDescriptor();
    }

    @Override
    public PeekCursor<IndexRow> scan(@Nullable BinaryTuplePrefix lowerBound, @Nullable BinaryTuplePrefix upperBound, int flags) {
        assertThreadAllowsToRead();

        return new ThreadAssertingPeekCursor<>(indexStorage.scan(lowerBound, upperBound, flags));
    }

    @Override
    public Cursor<IndexRow> readOnlyScan(@Nullable BinaryTuplePrefix lowerBound, @Nullable BinaryTuplePrefix upperBound, int flags) {
        return new ThreadAssertingCursor<>(indexStorage.readOnlyScan(lowerBound, upperBound, flags));
    }
}
