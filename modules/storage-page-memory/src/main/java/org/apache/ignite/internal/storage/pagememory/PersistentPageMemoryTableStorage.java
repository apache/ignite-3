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

import static org.apache.ignite.internal.storage.StorageUtils.groupId;

import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Implementation of {@link AbstractPageMemoryTableStorage} for persistent case.
 */
public class PersistentPageMemoryTableStorage extends AbstractPageMemoryTableStorage {
    /**
     * Constructor.
     *
     * @param tableCfg – Table configuration.
     * @param dataRegion – Data region for the table.
     */
    public PersistentPageMemoryTableStorage(
            TableConfiguration tableCfg,
            PersistentPageMemoryDataRegion dataRegion
    ) {
        super(tableCfg, dataRegion);
    }

    /** {@inheritDoc} */
    @Override
    public void start() throws StorageException {
        super.start();

        TableView tableView = tableCfg.value();

        try {
            ((PersistentPageMemoryDataRegion) dataRegion)
                    .filePageStoreManager()
                    .initialize(tableView.name(), groupId(tableView), tableView.partitions());
        } catch (IgniteInternalCheckedException e) {
            throw new StorageException("Error initializing file page stores for table: " + tableView.name(), e);
        }
    }

    /** {@inheritDoc} */
    @Override
    protected PageMemoryPartitionStorage createPartitionStorage(int partId) throws StorageException {
        // TODO: IGNITE-16641 continue

        return null;
    }
}
