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

import static org.apache.ignite.raft.jraft.test.TestUtils.mockEntry;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.ignite.raft.jraft.conf.ConfigurationManager;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.codec.LogEntryCodecFactory;
import org.apache.ignite.raft.jraft.entity.codec.v1.LogEntryV1CodecFactory;
import org.apache.ignite.raft.jraft.option.LogStorageOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.BaseStorageTest;
import org.apache.ignite.raft.jraft.storage.logit.option.StoreOptions;
import org.apache.ignite.raft.jraft.storage.logit.storage.factory.LogStoreFactory;
import org.apache.ignite.raft.jraft.storage.logit.storage.file.FileHeader;
import org.apache.ignite.raft.jraft.storage.logit.storage.file.index.IndexFile.IndexEntry;
import org.apache.ignite.raft.jraft.storage.logit.storage.file.index.IndexType;

public class BaseLogitStorageTest extends BaseStorageTest {
    protected StoreOptions storeOptions = new StoreOptions();
    protected int                  indexEntrySize;
    protected int                  headerSize;
    protected int                  indexFileSize;
    protected int                  segmentFileSize;
    protected ConfigurationManager confManager;
    protected LogEntryCodecFactory logEntryCodecFactory;

    protected LogStoreFactory logStoreFactory;

    protected final byte           segmentIndex = IndexType.IndexSegment.getType();

    public void setup() throws Exception {
        indexEntrySize = IndexEntry.INDEX_SIZE;
        headerSize = FileHeader.HEADER_SIZE;
        indexFileSize = headerSize + 10 * indexEntrySize;
        this.segmentFileSize = 300;

        storeOptions.setIndexFileSize(indexFileSize);
        storeOptions.setSegmentFileSize(segmentFileSize);
        storeOptions.setConfFileSize(segmentFileSize);
        this.logStoreFactory = new LogStoreFactory(storeOptions, new RaftOptions());

        this.confManager = new ConfigurationManager();
        this.logEntryCodecFactory = LogEntryV1CodecFactory.getInstance();
    }

    protected byte[] genData(final int index, final int term, int size) {
        final LogEntry entry = mockEntry(index, term, size - 16);
        final byte[] data = LogEntryV1CodecFactory.getInstance().encoder().encode(entry);
        assertEquals(size, data.length);
        return data;
    }

    protected LogStorageOptions newLogStorageOptions() {
        final LogStorageOptions opts = new LogStorageOptions();
        opts.setConfigurationManager(this.confManager);
        opts.setLogEntryCodecFactory(this.logEntryCodecFactory);
        return opts;
    }
}
