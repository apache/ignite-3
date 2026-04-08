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

package org.apache.ignite.raft.jraft.storage.logit;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.raft.configuration.LogStorageConfiguration;
import org.apache.ignite.internal.raft.storage.LogStorageManager;
import org.apache.ignite.internal.raft.storage.impl.DefaultLogStorageManager;
import org.apache.ignite.internal.raft.storage.segstore.SegmentLogStorageManager;
import org.apache.ignite.raft.jraft.JRaftServiceFactory;
import org.apache.ignite.raft.jraft.conf.ConfigurationManager;
import org.apache.ignite.raft.jraft.core.HybridLogJRaftServiceFactory;
import org.apache.ignite.raft.jraft.entity.codec.v1.LogEntryV1CodecFactory;
import org.apache.ignite.raft.jraft.option.LogStorageOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.BaseStorageTest;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.apache.ignite.raft.jraft.storage.logit.option.StoreOptions;
import org.apache.ignite.raft.jraft.storage.logit.storage.HybridLogStorage;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ConfigurationExtension.class)
class HybridLogStorageTest extends BaseStorageTest {
    private static final String STORAGE_RELATIVE_PATH = "log";

    private static final String NEW_STORAGE_RELATIVE_PATH = "new";

    private static final String GROUP_ID = "1_part_1";

    private @Nullable LogStorageManager oldStorageFactory;

    private LogStorageManager newStorageFactory;

    @AfterEach
    public void tearDown() throws Exception {
        if (oldStorageFactory != null) {
            oldStorageFactory.destroyLogStorage(GROUP_ID);
            oldStorageFactory.stopAsync();
        }

        if (newStorageFactory != null) {
            newStorageFactory.destroyLogStorage(GROUP_ID);
            newStorageFactory.stopAsync();
        }
    }

    @Test
    public void testTransferLogStorage() {
        Path storagePath = getStoragePath();

        oldStorageFactory = new DefaultLogStorageManager(storagePath.resolve("old"));
        newStorageFactory = new DefaultLogStorageManager(storagePath);

        testHybridStorage();
    }

    @Test
    public void testHybridStorageWithoutOldStorage() {
        Path storagePath = path.resolve(STORAGE_RELATIVE_PATH).resolve(NEW_STORAGE_RELATIVE_PATH);

        oldStorageFactory = null;
        newStorageFactory = new DefaultLogStorageManager(storagePath);

        testHybridStorage();
    }

    @Test
    public void testHybridStorageWithSegStore(@InjectConfiguration LogStorageConfiguration logStorageConfiguration) throws IOException {
        Path storagePath = getStoragePath();

        newStorageFactory = new SegmentLogStorageManager(
                "test",
                "test factory",
                storagePath,
                1,
                new NoOpFailureManager(),
                false,
                logStorageConfiguration
        );

        oldStorageFactory = new DefaultLogStorageManager(storagePath);

        testHybridStorage();
    }

    private void testHybridStorage() {
        RaftOptions raftOptions = new RaftOptions();
        raftOptions.setStartupOldStorage(oldStorageFactory != null);

        assertThat(newStorageFactory.startAsync(new ComponentContext()), willCompleteSuccessfully());

        long expectedThresholdIndex;

        int valueCount = 10;

        if (oldStorageFactory != null) {
            assertThat(oldStorageFactory.startAsync(new ComponentContext()), willCompleteSuccessfully());

            LogStorage oldStorage = oldStorageFactory.createLogStorage(GROUP_ID, new RaftOptions());

            assertTrue(oldStorage.init(logStorageOptions()));

            for (int i = 1; i <= valueCount; i++) {
                oldStorage.appendEntry(TestUtils.mockEntry(i, 1));
            }

            expectedThresholdIndex = oldStorage.getLastLogIndex() + 1;

            oldStorage.shutdown();
        } else {
            expectedThresholdIndex = 0;
        }

        HybridLogStorage hybridLogStorage = createHybridLogStorage(raftOptions, oldStorageFactory, newStorageFactory, getStoragePath());

        assertTrue(hybridLogStorage.init(logStorageOptions()));

        assertThat(hybridLogStorage.isOldStorageExist(), is(oldStorageFactory != null));

        // Checkpoint saved to disk when storage is started.
        assertTrue(Files.exists(statusCheckpointPath()));

        assertEquals(expectedThresholdIndex, hybridLogStorage.getThresholdIndex());

        for (int i = 0; i < valueCount; i++) {
            hybridLogStorage.appendEntry(TestUtils.mockEntry((int) (expectedThresholdIndex + i), 1));
        }

        assertEquals(expectedThresholdIndex + valueCount - 1, hybridLogStorage.getLastLogIndex());

        hybridLogStorage.truncatePrefix(expectedThresholdIndex);
        assertEquals(expectedThresholdIndex, hybridLogStorage.getFirstLogIndex());
        assertFalse(hybridLogStorage.isOldStorageExist());

        hybridLogStorage.shutdown();

        // Storages don't support restarting after shutdown, so we need to create new one.
        hybridLogStorage = createHybridLogStorage(raftOptions, oldStorageFactory, newStorageFactory, getStoragePath());

        assertTrue(hybridLogStorage.init(logStorageOptions()));
        assertFalse(hybridLogStorage.isOldStorageExist());
        assertEquals(0, hybridLogStorage.getThresholdIndex());
        assertEquals(expectedThresholdIndex, hybridLogStorage.getFirstLogIndex());
        assertEquals(expectedThresholdIndex + valueCount - 1, hybridLogStorage.getLastLogIndex());

        // Entries written to new storage must be readable after restart.
        for (int i = 0; i < valueCount; i++) {
            long index = expectedThresholdIndex + i;
            assertNotNull(hybridLogStorage.getEntry(index), "Entry missing at index " + index);
        }
    }

    private static HybridLogStorage createHybridLogStorage(
            RaftOptions raftOptions,
            @Nullable LogStorageManager oldStorageFactory,
            LogStorageManager newStorageFactory,
            Path storagePath
    ) {
        JRaftServiceFactory factory = new HybridLogJRaftServiceFactory(oldStorageFactory, newStorageFactory, storagePath);

        return (HybridLogStorage) factory.createLogStorage(GROUP_ID, raftOptions);
    }

    private Path getStoragePath() {
        return path.resolve(STORAGE_RELATIVE_PATH).resolve(NEW_STORAGE_RELATIVE_PATH);
    }

    private Path statusCheckpointPath() {
        return path.resolve(STORAGE_RELATIVE_PATH).resolve(NEW_STORAGE_RELATIVE_PATH).resolve(HybridLogStorage.STATUS_CHECKPOINT_PATH);
    }

    private static StoreOptions storeOptions() {
        StoreOptions storeOptions = new StoreOptions();
        storeOptions.setSegmentFileSize(512 * 1024);
        storeOptions.setConfFileSize(512 * 1024);
        storeOptions.setEnableWarmUpFile(false);
        return storeOptions;
    }

    private static LogStorageOptions logStorageOptions() {
        LogStorageOptions opts = new LogStorageOptions();
        opts.setConfigurationManager(new ConfigurationManager());
        opts.setLogEntryCodecFactory(LogEntryV1CodecFactory.getInstance());
        return opts;
    }
}
