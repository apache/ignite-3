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

package org.apache.ignite.internal.storage.pagememory.mv;

import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.FINISHED;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.internal.components.LongJvmPauseDetector;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryTableStorage;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith({ConfigurationExtension.class, WorkDirectoryExtension.class})
class PersistentPageMemoryMvPartitionStorageTest extends AbstractPageMemoryMvPartitionStorageTest {
    @WorkDirectory
    private Path workDir;

    @InjectConfiguration(value = "mock.checkpoint.checkpointDelayMillis = 0")
    private PersistentPageMemoryStorageEngineConfiguration engineConfig;

    @InjectConfiguration(
            value = "mock.tables.foo.dataStorage.name = " + PersistentPageMemoryStorageEngine.ENGINE_NAME
    )
    private TablesConfiguration tablesConfig;

    private LongJvmPauseDetector longJvmPauseDetector;

    private PersistentPageMemoryStorageEngine engine;

    private PersistentPageMemoryTableStorage table;

    @BeforeEach
    void setUp() {
        longJvmPauseDetector = new LongJvmPauseDetector("test", Loggers.forClass(LongJvmPauseDetector.class));

        longJvmPauseDetector.start();

        engine = new PersistentPageMemoryStorageEngine("test", engineConfig, ioRegistry, workDir, longJvmPauseDetector);

        engine.start();

        table = engine.createMvTable(tablesConfig.tables().get("foo"), tablesConfig);

        table.start();

        storage = table.createMvPartitionStorage(PARTITION_ID);
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(
                storage,
                table == null ? null : table::stop,
                engine == null ? null : engine::stop,
                longJvmPauseDetector == null ? null : longJvmPauseDetector::stop
        );
    }

    /** {@inheritDoc} */
    @Override
    int pageSize() {
        return engineConfig.pageSize().value();
    }

    @Test
    void testReadAfterRestart() throws Exception {
        RowId rowId = insert(binaryRow, txId);

        engine
                .checkpointManager()
                .forceCheckpoint("before_stop_engine")
                .futureFor(FINISHED)
                .get(1, TimeUnit.SECONDS);

        tearDown();

        setUp();

        assertRowMatches(binaryRow, read(rowId, HybridTimestamp.MAX_VALUE));
    }
}
