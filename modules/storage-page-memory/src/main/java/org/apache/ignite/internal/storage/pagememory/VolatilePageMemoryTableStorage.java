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
import org.apache.ignite.internal.storage.PartitionStorage;
import org.apache.ignite.internal.storage.StorageException;

/**
 * Implementation of {@link PageMemoryTableStorage} for in-memory case.
 */
public class VolatilePageMemoryTableStorage extends PageMemoryTableStorage {
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

        // TODO: Add FreeList.

        super.start();
    }

    @Override
    protected PartitionStorage createPartitionStorage(int partId) {
        return new PageMemoryPartitionStorage(partId);
    }
}
