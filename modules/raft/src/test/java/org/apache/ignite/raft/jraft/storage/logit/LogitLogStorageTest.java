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
import static org.apache.ignite.raft.jraft.entity.PeerId.emptyPeer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.raft.storage.logit.LogitLogStorageManager;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.raft.jraft.entity.EnumOutter;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.apache.ignite.raft.jraft.storage.impl.BaseLogStorageTest;
import org.apache.ignite.raft.jraft.storage.logit.option.StoreOptions;
import org.apache.ignite.raft.jraft.storage.logit.storage.LogitLogStorage;
import org.apache.ignite.raft.jraft.storage.logit.storage.db.IndexDB;
import org.apache.ignite.raft.jraft.storage.logit.storage.file.index.IndexType;
import org.apache.ignite.raft.jraft.storage.logit.util.Pair;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(WorkDirectoryExtension.class)
public class LogitLogStorageTest extends BaseLogStorageTest {
    private LogitLogStorageManager logStorageManager;

    @BeforeEach
    @Override
    public void setup() throws Exception {
        logStorageManager = new LogitLogStorageManager("test", testStoreOptions(), path);
        assertThat(logStorageManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

        super.setup();
    }

    private static StoreOptions testStoreOptions() {
        StoreOptions storeOptions = new StoreOptions();

        storeOptions.setSegmentFileSize(512 * 1024);
        storeOptions.setConfFileSize(512 * 1024);
        storeOptions.setEnableWarmUpFile(false);

        return storeOptions;
    }

    @AfterEach
    @Override
    public void teardown() {
        assertThat(logStorageManager.stopAsync(new ComponentContext()), willCompleteSuccessfully());

        super.teardown();
    }

    @Override
    protected LogStorage newLogStorage() {
        return logStorageManager.createLogStorage(uri(), new RaftOptions());
    }

    private String uri() {
        return this.path.toString();
    }

    /************************  Test consistency between dbs   ***********************************/

    @Test
    public void testAlignLogWhenLostIndex() {
        final List<LogEntry> entries = TestUtils.mockEntries(20);
        // Set 13 - 16 to be conf entry
        for (int i = 13; i <= 16; i++) {
            final LogEntry entry = entries.get(i);
            entry.setType(EnumOutter.EntryType.ENTRY_TYPE_CONFIGURATION);
            entry.setPeers(List.of(emptyPeer()));
        }
        this.logStorage.appendEntries(entries);
        assertEquals(19, this.logStorage.getLastLogIndex());

        // Truncate index db to the index 12, when logStorage reStart, the missing index will be recovered from log db;
        final IndexDB indexDB = ((LogitLogStorage) this.logStorage).getIndexDB();
        indexDB.truncateSuffix(12, 0);
        this.logStorage.shutdown();
        this.logStorage.init(newLogStorageOptions());

        assertEquals(19, this.logStorage.getLastLogIndex());
        for (int i = 0; i <= 19; i++) {
            final LogEntry entry = this.logStorage.getEntry(i);
            assertEquals(i, entry.getId().getIndex());
        }
    }

    @Test
    public void testAlignLogWhenMoreIndex() {
        final List<LogEntry> entries = TestUtils.mockEntries(15);
        this.logStorage.appendEntries(entries);
        // Append more index into indexDB
        final IndexDB indexDB = ((LogitLogStorage) this.logStorage).getIndexDB();
        long maxFlushPosition = 0;
        for (int i = 15; i <= 20; i++) {
            final Pair<Integer, Long> flushPair = indexDB.appendIndexAsync(i, 0, IndexType.IndexSegment);
            maxFlushPosition = Math.max(maxFlushPosition, flushPair.getSecond());
        }
        indexDB.waitForFlush(maxFlushPosition, 100);
        // Recover
        this.logStorage.shutdown();
        this.logStorage.init(newLogStorageOptions());

        // In this case, logitLogStorage will truncate indexdb to the index of 14
        final IndexDB indexDB1 = ((LogitLogStorage) this.logStorage).getIndexDB();
        assertEquals(14, indexDB1.getLastLogIndex());

        for (int i = 0; i <= 14; i++) {
            final LogEntry entry = this.logStorage.getEntry(i);
            assertEquals(i, entry.getId().getIndex());
        }
    }

    @Test
    public void destroysData() {
        logStorage.appendEntries(TestUtils.mockEntries(15));
        logStorage.shutdown();

        Path storagePath = logStorageManager.resolveLogStoragePath(uri());
        assertTrue(Files.isDirectory(storagePath));

        logStorageManager.destroyLogStorage(uri());

        assertFalse(Files.exists(storagePath));
    }
}
