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
import static org.apache.ignite.internal.worker.ThreadAssertions.assertThreadAllowsToWrite;

import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.worker.ThreadAssertingCursor;
import org.apache.ignite.internal.worker.ThreadAssertions;
import org.apache.ignite.internal.wrapper.Wrapper;
import org.jetbrains.annotations.Nullable;

/**
 * {@link IndexStorage} that performs thread assertions when doing read/write operations.
 *
 * @see ThreadAssertions
 */
abstract class ThreadAssertingIndexStorage implements IndexStorage, Wrapper {
    private final IndexStorage indexStorage;

    /** Constructor. */
    ThreadAssertingIndexStorage(IndexStorage indexStorage) {
        this.indexStorage = indexStorage;
    }

    @Override
    public Cursor<RowId> get(BinaryTuple key) throws StorageException {
        assertThreadAllowsToRead();

        return new ThreadAssertingCursor<>(indexStorage.get(key));
    }

    @Override
    public void put(IndexRow row) throws StorageException {
        assertThreadAllowsToWrite();

        indexStorage.put(row);
    }

    @Override
    public void remove(IndexRow row) throws StorageException {
        assertThreadAllowsToWrite();

        indexStorage.remove(row);
    }

    @Override
    public @Nullable RowId getNextRowIdToBuild() throws StorageException {
        return indexStorage.getNextRowIdToBuild();
    }

    @Override
    public void setNextRowIdToBuild(@Nullable RowId rowId) throws StorageException {
        assertThreadAllowsToWrite();

        indexStorage.setNextRowIdToBuild(rowId);
    }

    @Override
    public <T> T unwrap(Class<T> classToUnwrap) {
        return classToUnwrap.cast(indexStorage);
    }
}
