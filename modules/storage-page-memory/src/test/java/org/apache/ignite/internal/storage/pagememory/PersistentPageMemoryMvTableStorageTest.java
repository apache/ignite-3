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

package org.apache.ignite.internal.storage.pagememory;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.AbstractMvTableStorageTest;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for {@link PersistentPageMemoryTableStorage} class.
 */
@ExtendWith({ConfigurationExtension.class, WorkDirectoryExtension.class})
public class PersistentPageMemoryMvTableStorageTest extends AbstractMvTableStorageTest {
    @BeforeEach
    void setUp(
            @WorkDirectory
            Path workDir,
            @InjectConfiguration
            PersistentPageMemoryStorageEngineConfiguration engineConfig,
            @InjectConfiguration(
                    "mock.tables.foo{dataStorage.name = " + PersistentPageMemoryStorageEngine.ENGINE_NAME + "}"
            )
            TablesConfiguration tablesConfig,
            @InjectConfiguration
            DistributionZonesConfiguration distributionZonesConfiguration

    ) {
        var ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        initialize(new PersistentPageMemoryStorageEngine("test", engineConfig, ioRegistry, workDir, null), tablesConfig,
                distributionZonesConfiguration.defaultDistributionZone());
    }

    @Test
    @Override
    public void testDestroyPartition() throws Exception {
        super.testDestroyPartition();

        // Let's make sure that the checkpoint doesn't fail.
        ((PersistentPageMemoryStorageEngine) storageEngine).checkpointManager()
                .forceCheckpoint("after-test-destroy-partition")
                .futureFor(CheckpointState.FINISHED)
                .get(1, TimeUnit.SECONDS);
    }
}
