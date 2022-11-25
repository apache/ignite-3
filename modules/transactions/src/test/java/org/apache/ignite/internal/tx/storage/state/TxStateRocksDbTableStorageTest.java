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

package org.apache.ignite.internal.tx.storage.state;

import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.tx.storage.state.rocksdb.TxStateRocksDbTableStorage;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link TxStateRocksDbTableStorage} testing.
 */
@ExtendWith({ConfigurationExtension.class, WorkDirectoryExtension.class})
public class TxStateRocksDbTableStorageTest extends AbstractTxStateTableStorageTest {
    private static final IgniteLogger LOG = Loggers.forClass(TxStateRocksDbTableStorageTest.class);

    private ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
            new NamedThreadFactory("tx-state-storage-scheduled-pool", LOG)
    );

    private ExecutorService executorService = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors(),
            new NamedThreadFactory("tx-state-storage-pool", LOG)
    );

    @BeforeEach
    void setUp(
            @InjectConfiguration TableConfiguration tableConfig,
            @WorkDirectory Path workDir
    ) {
        initialize(new TxStateRocksDbTableStorage(tableConfig, workDir, scheduledExecutorService, executorService, () -> 100));
    }

    @AfterEach
    void tearDown() {
        IgniteUtils.shutdownAndAwaitTermination(scheduledExecutorService, 10, TimeUnit.SECONDS);
        IgniteUtils.shutdownAndAwaitTermination(executorService, 10, TimeUnit.SECONDS);
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18024")
    @Override
    public void testStartRebalance() throws Exception {
        super.testStartRebalance();
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18024")
    @Override
    public void testAbortRebalance() throws Exception {
        super.testAbortRebalance();
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-18024")
    @Override
    public void testFinishRebalance() throws Exception {
        super.testFinishRebalance();
    }
}
