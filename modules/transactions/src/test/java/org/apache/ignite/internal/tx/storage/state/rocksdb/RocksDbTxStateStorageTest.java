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

package org.apache.ignite.internal.tx.storage.state.rocksdb;

import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.tx.storage.state.AbstractTxStateStorageTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tx storage test for RocksDB implementation.
 */
@ExtendWith({WorkDirectoryExtension.class, ConfigurationExtension.class})
public class RocksDbTxStateStorageTest extends AbstractTxStateStorageTest {
    @WorkDirectory
    protected Path workDir;

    @InjectConfiguration("mock {partitions=3}")
    private TableConfiguration tableConfig;

    @Override
    protected TxStateRocksDbTableStorage createTableStorage() {
        return new TxStateRocksDbTableStorage(
                tableConfig,
                workDir,
                new ScheduledThreadPoolExecutor(1),
                Executors.newFixedThreadPool(1),
                () -> 1_000
        );
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18024")
    @Override
    public void testSuccessFullRebalance() throws Exception {
        super.testSuccessFullRebalance();
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18024")
    @Override
    public void testFailFullRebalance() throws Exception {
        super.testFailFullRebalance();
    }
}
