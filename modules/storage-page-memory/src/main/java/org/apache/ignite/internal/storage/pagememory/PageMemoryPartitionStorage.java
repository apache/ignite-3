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

package org.apache.ignite.internal.storage.pagememory;

import static org.apache.ignite.internal.pagememory.PageIdAllocator.MAX_PARTITION_ID;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import org.apache.ignite.internal.pagememory.tree.BplusTree;
import org.apache.ignite.internal.storage.DataRow;
import org.apache.ignite.internal.storage.InvokeClosure;
import org.apache.ignite.internal.storage.PartitionStorage;
import org.apache.ignite.internal.storage.SearchRow;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.util.Cursor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Storage implementation based on a {@link BplusTree}.
 */
public class PageMemoryPartitionStorage implements PartitionStorage {
    private final int partId;

    /**
     * Constructor.
     *
     * @param partId Partition id.
     */
    public PageMemoryPartitionStorage(int partId) {
        assert partId > 0 && partId < MAX_PARTITION_ID : partId;

        this.partId = partId;
    }

    /** {@inheritDoc} */
    @Override
    public int partitionId() {
        return partId;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable DataRow read(SearchRow key) throws StorageException {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Collection<DataRow> readAll(List<? extends SearchRow> keys) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public void write(DataRow row) throws StorageException {

    }

    /** {@inheritDoc} */
    @Override
    public void writeAll(List<? extends DataRow> rows) throws StorageException {

    }

    /** {@inheritDoc} */
    @Override
    public Collection<DataRow> insertAll(List<? extends DataRow> rows) throws StorageException {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public void remove(SearchRow key) throws StorageException {

    }

    /** {@inheritDoc} */
    @Override
    public Collection<SearchRow> removeAll(List<? extends SearchRow> keys) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Collection<DataRow> removeAllExact(List<? extends DataRow> keyValues) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public <T> @Nullable T invoke(SearchRow key, InvokeClosure<T> clo) throws StorageException {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public Cursor<DataRow> scan(Predicate<SearchRow> filter) throws StorageException {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public @NotNull CompletableFuture<Void> snapshot(Path snapshotPath) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public void restoreSnapshot(Path snapshotPath) {

    }

    /** {@inheritDoc} */
    @Override
    public void destroy() {

    }

    /** {@inheritDoc} */
    @Override
    public void close() throws Exception {

    }
}
