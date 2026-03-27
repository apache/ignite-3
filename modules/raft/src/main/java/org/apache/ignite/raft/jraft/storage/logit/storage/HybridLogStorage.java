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

package org.apache.ignite.raft.jraft.storage.logit.storage;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.raft.jraft.entity.LogEntry;
import org.apache.ignite.raft.jraft.option.LogStorageOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;
import org.apache.ignite.raft.jraft.storage.logit.storage.file.assit.HybridStorageStatusCheckpoint;
import org.apache.ignite.raft.jraft.util.OnlyForTest;

/**
 * HybridLogStorage is used to be compatible with new and old logStorage.
 */
public class HybridLogStorage implements LogStorage {
    /** Path to save the checkpoint of hybrid log storage status, which is used to indicate whether the old log storage exists. */
    public static final String STATUS_CHECKPOINT_PATH = "HybridStatusCheckpoint";

    private static final IgniteLogger LOG = Loggers.forClass(HybridLogStorage.class);

    private final HybridStorageStatusCheckpoint statusCheckpoint;
    private volatile boolean isOldStorageExist;
    private final LogStorage oldLogStorage;
    private final LogStorage newLogStorage;

    // The index which separates the oldStorage and newStorage.
    private long thresholdIndex;

    public HybridLogStorage(Path newStoragePath, RaftOptions raftOptions, LogStorage oldStorage, LogStorage newStorage) {
        this.newLogStorage = newStorage;
        this.oldLogStorage = oldStorage;
        String statusCheckpointPath = newStoragePath.resolve(STATUS_CHECKPOINT_PATH).toString();

        this.statusCheckpoint = new HybridStorageStatusCheckpoint(statusCheckpointPath, raftOptions);
    }

    @Override
    public boolean init( LogStorageOptions opts) {
        try {
            this.statusCheckpoint.load();
            this.isOldStorageExist = this.statusCheckpoint.isOldStorageExist;
            if (this.isOldStorageExist) {
                if (this.oldLogStorage != null) {
                    if (!this.oldLogStorage.init(opts)) {
                        LOG.warn("Init old log storage failed when startup hybridLogStorage");
                        return false;
                    }
                    long lastLogIndex = this.oldLogStorage.getLastLogIndex();
                    if (lastLogIndex == 0) {
                        shutdownOldLogStorage();
                    } else if (lastLogIndex > 0) {
                        // Still exists logs in oldLogStorage, need to wait snapshot
                        this.thresholdIndex = lastLogIndex + 1;
                        this.isOldStorageExist = true;
                        LOG.info(
                            "Still exists logs in oldLogStorage, lastIndex: {},  need to wait snapshot to truncate logs",
                            lastLogIndex);
                    }
                } else {
                    this.isOldStorageExist = false;
                }
            }

            if (!this.newLogStorage.init(opts)) {
                LOG.warn("Init new log storage failed when startup hybridLogStorage");
                return false;
            }

            saveStatusCheckpoint();

            return true;
        } catch (IOException e) {
            LOG.error("Error happen when load hybrid status checkpoint", e);
            return false;
        }
    }

    @Override
    public void shutdown() {
        if (isOldStorageExist()) {
            this.oldLogStorage.shutdown();
        }
        this.newLogStorage.shutdown();
    }

    @Override
    public long getFirstLogIndex() {
        if (isOldStorageExist()) {
            return this.oldLogStorage.getFirstLogIndex();
        }
        return this.newLogStorage.getFirstLogIndex();
    }

    @Override
    public long getLastLogIndex() {
        long newLastLogIndex = this.newLogStorage.getLastLogIndex();
        if (newLastLogIndex > 0) {
            return newLastLogIndex;
        }
        if (isOldStorageExist()) {
            return this.oldLogStorage.getLastLogIndex();
        }
        return 0;
    }

    @Override
    public LogEntry getEntry( long index) {
        if (index >= this.thresholdIndex) {
            return this.newLogStorage.getEntry(index);
        }
        if (isOldStorageExist()) {
            return this.oldLogStorage.getEntry(index);
        }
        return null;
    }

    @Override
    public long getTerm( long index) {
        if (index >= this.thresholdIndex) {
            return this.newLogStorage.getTerm(index);
        }
        if (isOldStorageExist()) {
            return this.oldLogStorage.getTerm(index);
        }
        return 0;
    }

    @Override
    public boolean appendEntry( LogEntry entry) {
        return this.newLogStorage.appendEntry(entry);
    }

    @Override
    public int appendEntries( List<LogEntry> entries) {
        return this.newLogStorage.appendEntries(entries);
    }

    @Override
    public boolean truncatePrefix( long firstIndexKept) {
        if (!isOldStorageExist()) {
            return this.newLogStorage.truncatePrefix(firstIndexKept);
        }

        if (firstIndexKept < this.thresholdIndex) {
            return this.oldLogStorage.truncatePrefix(firstIndexKept);
        }

        // When firstIndex >= thresholdIndex, we can reset the old storage the shutdown it.
        if (isOldStorageExist()) {
            this.oldLogStorage.reset(1);
            shutdownOldLogStorage();
            LOG.info("Truncate prefix at logIndex : {}, the thresholdIndex is : {}, shutdown oldLogStorage success!",
                firstIndexKept, this.thresholdIndex);
        }
        return this.newLogStorage.truncatePrefix(firstIndexKept);
    }

    @Override
    public boolean truncateSuffix( long lastIndexKept) {
        if (isOldStorageExist()) {
            if (!this.oldLogStorage.truncateSuffix(lastIndexKept)) {
                return false;
            }
        }
        return this.newLogStorage.truncateSuffix(lastIndexKept);
    }

    @Override
    public boolean reset( long nextLogIndex) {
        if (isOldStorageExist()) {
            if (!this.oldLogStorage.reset(nextLogIndex)) {
                return false;
            }
            shutdownOldLogStorage();
        }
        return this.newLogStorage.reset(nextLogIndex);
    }

    private void shutdownOldLogStorage() {
        this.oldLogStorage.shutdown();
        this.isOldStorageExist = false;
        this.thresholdIndex = 0;
        saveStatusCheckpoint();
    }

    private void saveStatusCheckpoint() {
        this.statusCheckpoint.isOldStorageExist = this.isOldStorageExist;
        try {
            // Save status
            this.statusCheckpoint.save();
        } catch ( IOException e) {
            LOG.error("Error happen when save hybrid status checkpoint", e);
        }
    }

    public boolean isOldStorageExist() {
        return this.isOldStorageExist;
    }

    @OnlyForTest
    public long getThresholdIndex() {
        return thresholdIndex;
    }
}

