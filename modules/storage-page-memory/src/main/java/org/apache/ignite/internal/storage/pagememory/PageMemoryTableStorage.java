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

import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.storage.PartitionStorage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.DataRegion;
import org.apache.ignite.internal.storage.engine.TableStorage;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.jetbrains.annotations.Nullable;

/**
 * Table storage implementation based on {@link PageMemory}.
 */
// TODO: IGNITE-16642 Support indexes.
public class PageMemoryTableStorage implements TableStorage {
    private final PageMemoryDataRegion dataRegion;

    private final TableConfiguration tableCfg;

    /**
     * Constructor.
     *
     * @param tableCfg – Table configuration.
     * @param dataRegion – Data region for the table.
     */
    public PageMemoryTableStorage(TableConfiguration tableCfg, PageMemoryDataRegion dataRegion) {
        this.dataRegion = dataRegion;
        this.tableCfg = tableCfg;
    }

    /** {@inheritDoc} */
    @Override
    public TableConfiguration configuration() {
        return tableCfg;
    }

    /** {@inheritDoc} */
    @Override
    public DataRegion dataRegion() {
        return dataRegion;
    }

    /** {@inheritDoc} */
    @Override
    public void start() throws StorageException {
        if (dataRegion.persistent()) {
            throw new UnsupportedOperationException("Persistent case is not supported yet.");
        }
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws StorageException {

    }

    /** {@inheritDoc} */
    @Override
    public void destroy() throws StorageException {

    }

    /** {@inheritDoc} */
    @Override
    public PartitionStorage getOrCreatePartition(int partId) throws StorageException {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable PartitionStorage getPartition(int partId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override
    public void dropPartition(int partId) throws StorageException {

    }

    /** {@inheritDoc} */
    @Override
    public SortedIndexStorage getOrCreateSortedIndex(String indexName) {
        throw new UnsupportedOperationException("Indexes are not supported yet.");
    }

    /** {@inheritDoc} */
    @Override
    public void dropIndex(String indexName) {
        throw new UnsupportedOperationException("Indexes are not supported yet.");
    }
}
