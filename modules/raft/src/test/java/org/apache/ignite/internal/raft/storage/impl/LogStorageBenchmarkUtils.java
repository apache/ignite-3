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

package org.apache.ignite.internal.raft.storage.impl;

import static org.apache.ignite.internal.raft.configuration.LogStorageConfigurationSchema.DEFAULT_MAX_CHECKPOINT_QUEUE_SIZE;
import static org.apache.ignite.internal.raft.configuration.LogStorageConfigurationSchema.DEFAULT_SEGMENT_FILE_SIZE_BYTES;
import static org.apache.ignite.internal.raft.configuration.LogStorageConfigurationSchema.DEFAULT_SOFT_LOG_SIZE_LIMIT_BYTES;
import static org.apache.ignite.internal.raft.configuration.LogStorageConfigurationSchema.UNSPECIFIED_MAX_LOG_ENTRY_SIZE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.failure.NoOpFailureManager;
import org.apache.ignite.internal.raft.configuration.LogStorageConfiguration;
import org.apache.ignite.internal.raft.configuration.LogStorageView;
import org.apache.ignite.internal.raft.storage.LogStorageManager;
import org.apache.ignite.internal.raft.storage.segstore.SegmentLogStorageManager;
import org.apache.ignite.raft.jraft.conf.ConfigurationManager;
import org.apache.ignite.raft.jraft.entity.EnumOutter;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.codec.v1.LogEntryV1CodecFactory;
import org.apache.ignite.raft.jraft.option.LogStorageOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;

/** Shared helpers for {@link LogStorage} JMH benchmarks. */
final class LogStorageBenchmarkUtils {
    static final String ROCKSDB = "rocksdb";

    static final String SEGSTORE = "segstore";

    /** Pre-written during setup; extra values truncated after each invocation. */
    static final int STARTING_ENTRIES = 30_000;

    /** Matches {@code DisruptorConfigurationSchema.DEFAULT_LOG_MANAGER_STRIPES_COUNT} used in production. */
    static final int SEGSTORE_STRIPES = 4;

    static final long TERM = 1;

    static final int PEER_INDEX = 1;

    static LogStorageManager createLogStorageManager(String storageType, Path path, boolean fsync) throws IOException {
        switch (storageType) {
            case ROCKSDB:
                return new DefaultLogStorageManager("test", "test", path, fsync, RocksDbLogStorageOptions.defaults());

            case SEGSTORE:
                return new SegmentLogStorageManager(
                        "test",
                        "test",
                        path,
                        SEGSTORE_STRIPES,
                        new NoOpFailureManager(),
                        fsync,
                        mockSegstoreConfig()
                );

            default:
                throw new IllegalArgumentException("Unsupported storage type: " + storageType);
        }
    }

    static LogStorage createAndInitLogStorage(LogStorageManager manager, String groupId, boolean fsync) {
        RaftOptions raftOptions = new RaftOptions();
        raftOptions.setSync(fsync);

        LogStorage logStorage = manager.createLogStorage(groupId, raftOptions);

        LogStorageOptions opts = new LogStorageOptions();
        opts.setConfigurationManager(new ConfigurationManager());
        opts.setLogEntryCodecFactory(LogEntryV1CodecFactory.getInstance());
        logStorage.init(opts);

        return logStorage;
    }

    static LogStorage seedGroup(LogStorageManager manager, String groupId, boolean fsync, int batchSize, byte[] data) {
        LogStorage logStorage = createAndInitLogStorage(manager, groupId, fsync);

        List<LogEntry> batch = new ArrayList<>(batchSize);
        for (int i = 0; i < STARTING_ENTRIES; i += batchSize) {
            batch.clear();
            int end = Math.min(i + batchSize, STARTING_ENTRIES);
            for (int j = i; j < end; j++) {
                LogEntry entry = new LogEntry(EnumOutter.EntryType.ENTRY_TYPE_NO_OP);
                entry.setId(new LogId(j, TERM));
                entry.setData(ByteBuffer.wrap(data));
                batch.add(entry);
            }
            logStorage.appendEntries(batch);
        }

        return logStorage;
    }

    private static LogStorageConfiguration mockSegstoreConfig() {
        LogStorageConfiguration config = mock(LogStorageConfiguration.class);
        LogStorageView view = mock(LogStorageView.class);

        when(config.value()).thenReturn(view);
        when(view.maxCheckpointQueueSize()).thenReturn(DEFAULT_MAX_CHECKPOINT_QUEUE_SIZE);
        when(view.segmentFileSizeBytes()).thenReturn((long) DEFAULT_SEGMENT_FILE_SIZE_BYTES);
        when(view.maxLogEntrySizeBytes()).thenReturn(UNSPECIFIED_MAX_LOG_ENTRY_SIZE);
        when(view.softLogSizeLimitBytes()).thenReturn(DEFAULT_SOFT_LOG_SIZE_LIMIT_BYTES);

        return config;
    }
}
