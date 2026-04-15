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
    /** Group ID for metastorage. */
    public static final long METASTORAGE_GROUP_ID = 1;

    /** Group ID for CMG. */
    public static final long CMG_GROUP_ID = 2;

    /** Offset so that (objectId=0, partitionId=0) maps to 3, avoiding collision with metastorage (1) and cmg (2). */
    public static final long SPECIAL_GROUP_ID_OFFSET = 3;

    private static final Pattern PARTITION_GROUP_ID_PATTERN = Pattern.compile("_part_");

    private final SegmentFileManager fileManager;

    /** Constructor. */
    public SegmentLogStorageManager(
            String nodeName,
            String storageName,
            Path logStoragePath,
            int stripes,
            FailureProcessor failureProcessor,
            boolean fsync,
            LogStorageConfiguration storageConfiguration
    ) throws IOException {
        this.fileManager = new SegmentFileManager(
                nodeName,
                storageName,
                logStoragePath,
                stripes,
                failureProcessor,
                fsync,
                storageConfiguration
        );
    }

    @Override
    public LogStorage createLogStorage(String raftNodeStorageId, RaftOptions raftOptions) {
        return new SegstoreLogStorage(convertNodeId(raftNodeStorageId), fileManager);
    }

    @Override
    public void destroyLogStorage(String raftNodeStorageId) {
        // TODO IGNITE-28527 Implement.
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

    /**
     * Converts a raft node storage ID string to a unique positive long, used to identify a logical log within the segment file manager.
     *
     * <p>For partition groups ({@code "{objectId}_part_{partitionId}-{peerIdx}"}):
     * {@code result = (objectId << 32 | partitionId) + SPECIAL_GROUP_ID_OFFSET}.
     * {@code objectId} and {@code partitionId} are non-negative ints, so the result is always positive and unique
     * per {@code (objectId, partitionId)} pair.
     *
     * <p>{@code peerIdx} is not encoded because a single Ignite node participates as exactly one peer per raft group,
     * so {@code (objectId, partitionId)} is already unique within one {@link SegmentLogStorageManager}.
     */
    // TODO IGNITE-26977 Revise after changing partition ID from int to long.
    static long convertNodeId(String nodeId) {
        // {groupId}-{peerIdx}. Peer index suffix is mandatory for all valid raft node storage IDs.
        int lastHyphen = nodeId.lastIndexOf('-');

        if (lastHyphen <= 0) {
            // TODO IGNITE-28525 Validate node IDs instead of allowing any value.
            return Integer.toUnsignedLong(nodeId.hashCode()) + SPECIAL_GROUP_ID_OFFSET;
        }

        String groupName = nodeId.substring(0, lastHyphen);

        if ("metastorage_group".equals(groupName)) {
            return METASTORAGE_GROUP_ID;
        }

        if ("cmg_group".equals(groupName)) {
            return CMG_GROUP_ID;
        }

        String[] parts = PARTITION_GROUP_ID_PATTERN.split(groupName);

        if (parts.length == 2) {
            int objectId = Integer.parseInt(parts[0]);
            int partitionId = Integer.parseInt(parts[1]);

            return ((long) objectId << 32 | Integer.toUnsignedLong(partitionId)) + SPECIAL_GROUP_ID_OFFSET;
        }

        // TODO IGNITE-28525 Validate node IDs instead of allowing any value.
        return Integer.toUnsignedLong(nodeId.hashCode()) + SPECIAL_GROUP_ID_OFFSET;
    }
}
