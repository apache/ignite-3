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

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.raft.configuration.LogStorageConfiguration;
import org.apache.ignite.internal.raft.storage.LogStorageManager;
import org.apache.ignite.internal.raft.storage.impl.LogStorageException;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.storage.LogStorage;

/**
 * Log storage manager for {@link SegstoreLogStorage} instances.
 */
public class SegmentLogStorageManager implements LogStorageManager {
    private static final Pattern PARTITION_GROUP_ID_PATTERN = Pattern.compile("_part_");

    private final SegmentFileManager fileManager;

    public SegmentLogStorageManager(
            String nodeName,
            Path logStoragePath,
            int stripes,
            FailureProcessor failureProcessor,
            boolean fsync,
            LogStorageConfiguration storageConfiguration
    ) throws IOException {
        this.fileManager = new SegmentFileManager(nodeName, logStoragePath, stripes, failureProcessor, fsync, storageConfiguration);
    }

    @Override
    public LogStorage createLogStorage(String groupId, RaftOptions raftOptions) {
        return new SegstoreLogStorage(convertGroupId(groupId), fileManager);
    }

    @Override
    public void destroyLogStorage(String groupId) {
        try {
            fileManager.reset(convertGroupId(groupId), 1);
        } catch (IOException e) {
            throw new LogStorageException("Failed to destroy log storage for group " + groupId, e);
        }
    }

    @Override
    public Set<String> raftNodeStorageIdsOnDisk() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long totalBytesOnDisk() {
        return fileManager.logSizeBytes();
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        try {
            fileManager.start();

            return nullCompletedFuture();
        } catch (IOException e) {
            return failedFuture(new LogStorageException("Couldn't start SegmentLogStorageManager", e));
        }
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        try {
            fileManager.close();

            return nullCompletedFuture();
        } catch (Exception e) {
            return failedFuture(new LogStorageException("Couldn't stop SegmentLogStorageManager", e));
        }
    }

    private static long convertGroupId(String groupId) {
        if ("metastorage_group".equals(groupId)) {
            return 1;
        }

        if ("cmg_group".equals(groupId)) {
            return 2;
        }

        String[] partitionGroupIdArray = PARTITION_GROUP_ID_PATTERN.split(groupId);

        if (partitionGroupIdArray.length == 2) {
            return Long.parseLong(partitionGroupIdArray[0]) << 32 | Long.parseLong(partitionGroupIdArray[1]);
        } else {
            throw new IllegalArgumentException("Invalid groupId: " + groupId);
        }
    }
}
