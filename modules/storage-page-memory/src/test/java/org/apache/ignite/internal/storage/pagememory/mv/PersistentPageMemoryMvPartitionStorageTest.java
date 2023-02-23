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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.engine.StorageEngine;
import org.apache.ignite.internal.storage.pagememory.PersistentPageMemoryStorageEngine;
import org.apache.ignite.internal.storage.pagememory.configuration.schema.PersistentPageMemoryStorageEngineConfiguration;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(WorkDirectoryExtension.class)
class PersistentPageMemoryMvPartitionStorageTest extends AbstractPageMemoryMvPartitionStorageTest {
    @InjectConfiguration("mock.checkpoint.checkpointDelayMillis = 0")
    private PersistentPageMemoryStorageEngineConfiguration engineConfig;

    @WorkDirectory
    private Path workDir;

    @Override
    protected StorageEngine createEngine() {
        return new PersistentPageMemoryStorageEngine("test", engineConfig, ioRegistry, workDir, null);
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
        ((PersistentPageMemoryStorageEngine) engine)
                .checkpointManager()
                .forceCheckpoint("before_stop_engine")
                .futureFor(FINISHED)
                .get(1, TimeUnit.SECONDS);

        tearDown();

        setUp();
    }

    @Test
    void groupConfigIsPersisted() throws Exception {
        byte[] originalConfig = {1, 2, 3};

        storage.runConsistently(() -> {
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

        storage.runConsistently(() -> {
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

        storage.runConsistently(() -> {
            storage.committedGroupConfiguration(originalConfigOfMoreThanOnePage);

            return null;
        });

        byte[] configWhichFitsInOnePage = {1, 2, 3};

        storage.runConsistently(() -> {
            storage.committedGroupConfiguration(configWhichFitsInOnePage);

            return null;
        });

        restartStorage();

        byte[] readConfig = storage.committedGroupConfiguration();

        assertThat(readConfig, is(equalTo(configWhichFitsInOnePage)));
    }
}
