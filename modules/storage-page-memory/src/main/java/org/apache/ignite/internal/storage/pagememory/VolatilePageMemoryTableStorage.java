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

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;
import static org.apache.ignite.internal.pagememory.PageIdAllocator.INDEX_PARTITION;

import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.pagememory.evict.PageEvictionTrackerNoOp;
import org.apache.ignite.internal.pagememory.util.PageLockListenerNoOp;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Implementation of {@link PageMemoryTableStorage} for in-memory case.
 */
class VolatilePageMemoryTableStorage extends PageMemoryTableStorage {
    private TableFreeList freeList;

    /**
     * Constructor.
     *
     * @param tableCfg – Table configuration.
     * @param dataRegion – Data region for the table.
     */
    public VolatilePageMemoryTableStorage(TableConfiguration tableCfg, PageMemoryDataRegion dataRegion) {
        super(tableCfg, dataRegion);
    }

    /** {@inheritDoc} */
    @Override
    public void start() throws StorageException {
        assert !dataRegion.persistent() : "Persistent data region : " + dataRegion;

        TableView tableView = tableCfg.value();

        try {
            int grpId = tableView.name().hashCode();

            long metaPageId = dataRegion.pageMemory().allocatePage(grpId, INDEX_PARTITION, FLAG_AUX);

            freeList = new TableFreeList(
                    grpId,
                    dataRegion.pageMemory(),
                    PageLockListenerNoOp.INSTANCE,
                    metaPageId,
                    true,
                    null,
                    PageEvictionTrackerNoOp.INSTANCE
            ) {
                /** {@inheritDoc} */
                @Override
                protected long allocatePageNoReuse() throws IgniteInternalCheckedException {
                    return pageMem.allocatePage(grpId, INDEX_PARTITION, defaultPageFlag);
                }
            };

            autoCloseables.add(freeList::close);
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("", e);
        }

        super.start();
    }

    /** {@inheritDoc} */
    @Override
    protected PageMemoryPartitionStorage createPartitionStorage(int partId) throws StorageException {
        return new PageMemoryPartitionStorage(partId, tableCfg, dataRegion, freeList);
    }
}
