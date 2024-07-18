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

import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.FINISHED;
import static org.apache.ignite.internal.schema.BinaryRowMatcher.isRow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

import java.nio.file.Path;
import org.apache.ignite.internal.components.LogSyncer;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.configurations.StorageConfiguration;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.storage.engine.StorageTableDescriptor;
import org.apache.ignite.internal.storage.index.StorageIndexDescriptorSupplier;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.IgniteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(WorkDirectoryExtension.class)
class PersistentPageMemoryMvPartitionStorageTest extends AbstractPageMemoryMvPartitionStorageTest {
    @InjectConfiguration("mock.checkpoint.checkpointDelayMillis = 0")
    private PersistentPageMemoryStorageEngineConfiguration engineConfig;

    @InjectConfiguration("mock.profiles.default = {engine = \"aipersist\"}")
    private StorageConfiguration storageConfiguration;

    @WorkDirectory
    private Path workDir;

    private PersistentPageMemoryStorageEngine engine;

    private MvTableStorage table;

    @BeforeEach
    void setUp() {
        var ioRegistry = new PageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        engine = new PersistentPageMemoryStorageEngine(
                "test",
                engineConfig,
                storageConfiguration,
                ioRegistry,
                workDir,
                null,
                mock(FailureProcessor.class),
                mock(LogSyncer.class)
        );

        engine.start();

        table = engine.createMvTable(
                new StorageTableDescriptor(1, DEFAULT_PARTITION_COUNT, DEFAULT_STORAGE_PROFILE),
                mock(StorageIndexDescriptorSupplier.class)
        );

        initialize(table);
    }

    @AfterEach
    @Override
    protected void tearDown() throws Exception {
        super.tearDown();

        IgniteUtils.closeAllManually(
                table,
                engine == null ? null : engine::stop
        );
    }

    @Override
    int pageSize() {
        return engineConfig.pageSize().value();
    }

    @Test
    void testReadAfterRestart() throws Exception {
        RowId rowId = insert(binaryRow, txId);

        restartStorage();

        assertThat(read(rowId, HybridTimestamp.MAX_VALUE), isRow(binaryRow));
    }

    private void restartStorage() throws Exception {
        assertThat(
                engine.checkpointManager().forceCheckpoint("before_stop_engine").futureFor(FINISHED),
                willCompleteSuccessfully()
        );

        tearDown();

        setUp();
    }

    @Test
    void groupConfigIsPersisted() throws Exception {
        byte[] originalConfig = {1, 2, 3};

        storage.runConsistently(locker -> {
            storage.committedGroupConfiguration(originalConfig);

            return null;
        });

        restartStorage();

        byte[] readConfig = storage.committedGroupConfiguration();

        assertThat(readConfig, is(equalTo(originalConfig)));
    }

    @Test
    void groupConfigWhichDoesNotFitInOnePageIsPersisted() throws Exception {
        byte[] originalConfig = configThatDoesNotFitInOnePage();

        storage.runConsistently(locker -> {
            storage.committedGroupConfiguration(originalConfig);

            return null;
        });

        restartStorage();

        byte[] readConfig = storage.committedGroupConfiguration();

        assertThat(readConfig, is(equalTo(originalConfig)));
    }

    private static byte[] configThatDoesNotFitInOnePage() {
        byte[] originalconfig = new byte[1_000_000];

        for (int i = 0; i < originalconfig.length; i++) {
            originalconfig[i] = (byte) (i % 100);
        }

        return originalconfig;
    }

    @Test
    void groupConfigShorteningWorksCorrectly() throws Exception {
        byte[] originalConfigOfMoreThanOnePage = configThatDoesNotFitInOnePage();

        assertThat(originalConfigOfMoreThanOnePage.length, is(greaterThan(5 * pageSize())));

        storage.runConsistently(locker -> {
            storage.committedGroupConfiguration(originalConfigOfMoreThanOnePage);

            return null;
        });

        byte[] configWhichFitsInOnePage = {1, 2, 3};

        storage.runConsistently(locker -> {
            storage.committedGroupConfiguration(configWhichFitsInOnePage);

            return null;
        });

        restartStorage();

        byte[] readConfig = storage.committedGroupConfiguration();

        assertThat(readConfig, is(equalTo(configWhichFitsInOnePage)));
    }
}
