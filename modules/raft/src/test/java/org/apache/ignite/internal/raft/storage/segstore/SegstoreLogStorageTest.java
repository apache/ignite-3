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

package org.apache.ignite.internal.raft.storage.segstore;

import static org.apache.ignite.internal.util.IgniteUtils.closeAllManually;

import java.io.IOException;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.apache.ignite.raft.jraft.storage.impl.BaseLogStorageTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;

class SegstoreLogStorageTest extends BaseLogStorageTest {
    private static final int SEGMENT_SIZE = 512 * 1024; // Same as in JRaft tests.

    private static final long GROUP_ID = 1000;

    private static final String NODE_NAME = "test";

    private SegmentFileManager segmentFileManager;

    @AfterEach
    void tearDown() throws Exception {
        closeAllManually(segmentFileManager);
    }

    @Override
    protected LogStorage newLogStorage() {
        try {
            segmentFileManager = new SegmentFileManager(NODE_NAME, path, SEGMENT_SIZE, 1, new NoOpFailureManager());

            logStorage = new SegstoreLogStorage(GROUP_ID, segmentFileManager);

            segmentFileManager.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return logStorage;
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-26286")
    @Override
    public void testReset() {
        super.testReset();
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-26285")
    @Override
    public void testTruncatePrefix() {
        super.testTruncatePrefix();
    }
}
