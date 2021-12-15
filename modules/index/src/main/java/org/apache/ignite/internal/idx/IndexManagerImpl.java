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

package org.apache.ignite.internal.idx;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.configuration.schemas.store.DataStorageConfiguration;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.idx.event.IndexEvent;
import org.apache.ignite.internal.idx.event.IndexEventParameters;
import org.apache.ignite.internal.manager.AbstractProducer;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.lang.NodeStoppingException;

/**
 * Internal index manager facade provides low-level methods for indexes operations.
 */
public class IndexManagerImpl extends AbstractProducer<IndexEvent, IndexEventParameters> implements IndexManager, IgniteComponent {
    private final TablesConfiguration tablesCfg;
    private final DataStorageConfiguration dataStorageCfg;
    private final Path idxsStoreDir;
    private final TxManager txManager;

    /**
     * Constructor.
     *
     * @param tablesCfg          Tables configuration.
     * @param dataStorageCfg     Data storage configuration.
     * @param idxsStoreDir       Indexes store directory.
     * @param txManager          TX manager.
     */
    public IndexManagerImpl(
            TablesConfiguration tablesCfg,
            DataStorageConfiguration dataStorageCfg,
            Path idxsStoreDir,
            TxManager txManager
    ) {
        this.tablesCfg = tablesCfg;
        this.dataStorageCfg = dataStorageCfg;
        this.idxsStoreDir = idxsStoreDir;
        this.txManager = txManager;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() throws Exception {
    }

    @Override
    public List<SortedIndex> indexes(UUID tblId) throws NodeStoppingException {
        return null;
    }
}
