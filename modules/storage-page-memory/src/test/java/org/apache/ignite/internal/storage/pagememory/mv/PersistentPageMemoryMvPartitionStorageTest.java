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

package org.apache.ignite.internal.storage.pagememory.mv;

import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.FINISHED;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.schemas.store.UnknownDataStorageConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.ConstantValueDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.FunctionCallDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.HashIndexConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.NullValueDefaultConfigurationSchema;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.internal.components.LongJvmPauseDetector;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.configuration.schema.UnsafeMemoryAllocatorConfigurationSchema;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryTableStorage;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryDataStorageChange;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryDataStorageConfigurationSchema;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryDataStorageView;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineConfigurationSchema;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PersistentPageMemoryMvPartitionStorageTest extends AbstractPageMemoryMvPartitionStorageTest {
    @InjectConfiguration(
            value = "mock.checkpoint.checkpointDelayMillis = 0",
            polymorphicExtensions = UnsafeMemoryAllocatorConfigurationSchema.class
    )
    private PersistentPageMemoryStorageEngineConfiguration engineConfig;

    @InjectConfiguration(
            name = "table",
            polymorphicExtensions = {
                    HashIndexConfigurationSchema.class,
                    UnknownDataStorageConfigurationSchema.class,
                    PersistentPageMemoryDataStorageConfigurationSchema.class,
                    ConstantValueDefaultConfigurationSchema.class,
                    FunctionCallDefaultConfigurationSchema.class,
                    NullValueDefaultConfigurationSchema.class,
            }
    )
    private TableConfiguration tableCfg;

    private LongJvmPauseDetector longJvmPauseDetector;

    private PersistentPageMemoryStorageEngine engine;

    private PersistentPageMemoryTableStorage table;

    @BeforeEach
    void setUp() throws Exception {
        longJvmPauseDetector = new LongJvmPauseDetector("test", Loggers.forClass(LongJvmPauseDetector.class));

        longJvmPauseDetector.start();

        engine = new PersistentPageMemoryStorageEngine("test", engineConfig, ioRegistry, workDir, longJvmPauseDetector);

        engine.start();

        tableCfg
                .change(c -> c.changeDataStorage(dsc -> dsc.convert(PersistentPageMemoryDataStorageChange.class)))
                .get(1, TimeUnit.SECONDS);

        assertEquals(
                PersistentPageMemoryStorageEngineConfigurationSchema.DEFAULT_DATA_REGION_NAME,
                ((PersistentPageMemoryDataStorageView) tableCfg.dataStorage().value()).dataRegion()
        );

        table = engine.createMvTable(tableCfg);
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

        assertRowMatches(binaryRow, read(rowId, txId));
    }
}
