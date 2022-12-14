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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.pagememory.persistence.checkpoint.CheckpointState.FINISHED;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.ignite.internal.components.LongJvmPauseDetector;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.RaftGroupConfiguration;
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

    private PersistentPageMemoryMvPartitionStorage pageMemStorage;

    @BeforeEach
    void setUp() {
        longJvmPauseDetector = new LongJvmPauseDetector("test", Loggers.forClass(LongJvmPauseDetector.class));

        longJvmPauseDetector.start();

        engine = new PersistentPageMemoryStorageEngine("test", engineConfig, ioRegistry, workDir, longJvmPauseDetector);

        engine.start();

        table = engine.createMvTable(tablesConfig.tables().get("foo"), tablesConfig);

        table.start();

        pageMemStorage = table.createMvPartitionStorage(PARTITION_ID);
        storage = pageMemStorage;

        ((PersistentPageMemoryMvPartitionStorage) storage).start();
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(
                storage::close,
                table == null ? null : table::stop,
                engine == null ? null : engine::stop,
                longJvmPauseDetector == null ? null : longJvmPauseDetector::stop
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

        assertRowMatches(binaryRow, read(rowId, HybridTimestamp.MAX_VALUE));
    }

    private void restartStorage() throws Exception {
        engine
                .checkpointManager()
                .forceCheckpoint("before_stop_engine")
                .futureFor(FINISHED)
                .get(1, TimeUnit.SECONDS);

        tearDown();

        setUp();
    }

    @Test
    void groupConfigIsPersisted() throws Exception {
        RaftGroupConfiguration originalConfig = new RaftGroupConfiguration(
                List.of("peer1", "peer2"),
                List.of("old-peer1", "old-peer2"),
                List.of("learner1", "learner2"),
                List.of("old-learner1", "old-learner2")
        );

        storage.runConsistently(() -> {
            storage.committedGroupConfiguration(originalConfig);

            return null;
        });

        restartStorage();

        RaftGroupConfiguration readConfig = storage.committedGroupConfiguration();

        assertThat(readConfig, is(equalTo(originalConfig)));
    }

    @Test
    void groupConfigWhichDoesNotFitInOnePageIsPersisted() throws Exception {
        List<String> oneMbOfPeers = IntStream.range(0, 100_000)
                .mapToObj(n -> String.format("peer%06d", n))
                .collect(toList());

        RaftGroupConfiguration originalConfig = new RaftGroupConfiguration(
                oneMbOfPeers,
                List.of("old-peer1", "old-peer2"),
                List.of("learner1", "learner2"),
                List.of("old-learner1", "old-learner2")
        );

        storage.runConsistently(() -> {
            storage.committedGroupConfiguration(originalConfig);

            return null;
        });

        restartStorage();

        RaftGroupConfiguration readConfig = storage.committedGroupConfiguration();

        assertThat(readConfig, is(equalTo(originalConfig)));
    }

    @Test
    void groupConfigShorteningWorksCorrectly() throws Exception {
        List<String> oneMbOfPeers = IntStream.range(0, 100_000)
                .mapToObj(n -> String.format("peer%06d", n))
                .collect(toList());

        RaftGroupConfiguration originalConfigOfMoreThanOnePage = new RaftGroupConfiguration(
                oneMbOfPeers,
                List.of("old-peer1", "old-peer2"),
                List.of("learner1", "learner2"),
                List.of("old-learner1", "old-learner2")
        );

        storage.runConsistently(() -> {
            storage.committedGroupConfiguration(originalConfigOfMoreThanOnePage);

            return null;
        });

        RaftGroupConfiguration configWhichFitsInOnePage = new RaftGroupConfiguration(
                List.of("peer1", "peer2"),
                List.of("old-peer1", "old-peer2"),
                List.of("learner1", "learner2"),
                List.of("old-learner1", "old-learner2")
        );

        storage.runConsistently(() -> {
            storage.committedGroupConfiguration(configWhichFitsInOnePage);

            return null;
        });

        restartStorage();

        RaftGroupConfiguration readConfig = storage.committedGroupConfiguration();

        assertThat(readConfig, is(equalTo(configWhichFitsInOnePage)));
    }
}
