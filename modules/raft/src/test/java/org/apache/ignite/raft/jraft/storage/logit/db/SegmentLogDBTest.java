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

package org.apache.ignite.raft.jraft.storage.logit.db;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.codec.v1.LogEntryV1CodecFactory;
import org.apache.ignite.raft.jraft.storage.logit.BaseLogitStorageTest;
import org.apache.ignite.raft.jraft.storage.logit.storage.db.AbstractDB;
import org.apache.ignite.raft.jraft.storage.logit.storage.db.SegmentLogDB;
import org.apache.ignite.raft.jraft.storage.logit.util.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SegmentLogDBTest extends BaseLogitStorageTest {
    private SegmentLogDB segmentLogDB;
    private String       segmentStorePath;

    private ScheduledExecutorService checkpointExecutor;

    @BeforeEach
    @Override
    public void setup() throws Exception {
        super.setup();
        checkpointExecutor = Executors.newSingleThreadScheduledExecutor();

        this.segmentStorePath = this.path + File.separator + "segment";
        Files.createDirectories(Path.of(segmentStorePath));
        this.init();
    }

    public void init() {
        this.segmentLogDB = new SegmentLogDB(this.segmentStorePath, checkpointExecutor);
        this.segmentLogDB.init(this.logStoreFactory);
    }

    @AfterEach
    public void teardown() throws Exception {
        this.segmentLogDB.shutdown();
        checkpointExecutor.shutdown();
    }

    @Test
    public void testAppendLog() throws Exception {
        this.segmentLogDB.startServiceManager();
        // The default file size is 300
        // One entry size = 24 + 6 = 30, so this case will create three log file
        Pair<Integer, Long> posPair = null;
        for (int i = 0; i < 20; i++) {
            final byte[] data = genData(i, 0, 24);
            posPair = this.segmentLogDB.appendLogAsync(i, data);
        }
        this.segmentLogDB.waitForFlush(posPair.getSecond(), 100);
        assertEquals(0, this.segmentLogDB.getFirstLogIndex());
        assertEquals(19, this.segmentLogDB.getLastLogIndex());
        assertEquals((600 + 26 + 2 * 30), this.segmentLogDB.getFlushedPosition());
    }

    @Test
    public void testIterator() throws Exception {
        testAppendLog();
        // Read from the fifth entry, pos = 26 + 30 * 4 = 146
        final AbstractDB.LogEntryIterator iterator = this.segmentLogDB.iterator(LogEntryV1CodecFactory.getInstance()
            .decoder(), 5, 146);
        LogEntry entry;
        int index = 4;
        while ((entry = iterator.next()) != null) {
            assertEquals(index, entry.getId().getIndex());
            index++;
        }
    }

    @Test
    public void testRecover() throws Exception {
        this.segmentLogDB.startServiceManager();
        final byte[] data = genData(1, 0, 150);
        final byte[] data2 = genData(2, 0, 100);
        final byte[] data3 = genData(3, 0, 100);
        {
            // Write first file , one segment file size = 300
            this.segmentLogDB.appendLogAsync(1, data);
            this.segmentLogDB.appendLogAsync(2, data2);
            // Write second file
            final Pair<Integer, Long> posPair = this.segmentLogDB.appendLogAsync(3, data3);
            this.segmentLogDB.waitForFlush(posPair.getSecond(), 100);
        }

        final byte[] log = this.segmentLogDB.lookupLog(3, this.headerSize);
        assertArrayEquals(data3, log);
        {
            this.segmentLogDB.shutdown();
            this.init();
            this.segmentLogDB.recover();
            // Last flush position = one segment file size (300) + header(26) + log3Size(2 + 4 + 100) = 432
            assertEquals(432, this.segmentLogDB.getFlushedPosition());
        }
        {
            final byte[] log1 = this.segmentLogDB.lookupLog(1, this.headerSize);
            assertArrayEquals(data, log1);
            final byte[] log3 = this.segmentLogDB.lookupLog(3, this.headerSize);
            assertArrayEquals(data3, log3);
        }
    }

}
