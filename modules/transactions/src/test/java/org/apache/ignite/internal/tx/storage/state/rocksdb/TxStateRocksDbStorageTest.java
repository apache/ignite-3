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

package org.apache.ignite.internal.tx.storage.state.rocksdb;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.internal.tx.storage.state.TxnStateStorageAbstractTest;
import org.apache.ignite.internal.tx.storage.state.TxnStateTableStorage;

/**
 * Tx storage test for RocksDB implementation.
 */
public class TxStateRocksDbStorageTest extends TxnStateStorageAbstractTest {
    private TxnStateTableStorage txnStateTableStorage = null;

    /** {@inheritDoc} */
    @Override protected TxnStateTableStorage createStorage() {
        if (txnStateTableStorage != null) {
            return txnStateTableStorage;
        }

        TableView tableView = mock(TableView.class);
        when(tableView.name()).thenReturn("testTable");
        when(tableView.partitions()).thenReturn(1);

        TableConfiguration tableCfg = mock(TableConfiguration.class);
        when(tableCfg.value()).thenReturn(tableView);

        txnStateTableStorage = new TxnStateRocksDbTableStorage(
                tableCfg,
                workDir,
                new ScheduledThreadPoolExecutor(1),
                Executors.newFixedThreadPool(1),
                () -> 1000
        );

        txnStateTableStorage.start();

        return txnStateTableStorage;
    }

    /** {@inheritDoc} */
    @Override protected void destroyStorage() {
        txnStateTableStorage.destroy();
    }
}
