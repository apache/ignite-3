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

package org.apache.ignite.internal.partition.replicator.raft.snapshot;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.startup.StartupPartitionSnapshotReader;
import org.apache.ignite.internal.raft.storage.SnapshotStorageFactory;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.option.SnapshotCopierOptions;
import org.apache.ignite.raft.jraft.storage.SnapshotStorage;
import org.apache.ignite.raft.jraft.storage.SnapshotThrottle;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotCopier;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotWriter;
import org.jetbrains.annotations.Nullable;

/**
 * {@link SnapshotStorageFactory} implementation wrapping a {@link PartitionSnapshotStorage}.
 */
public class PartitionSnapshotStorageFactory implements SnapshotStorageFactory {
    private final PartitionSnapshotStorage snapshotStorage;

    /** Constructor. */
    public PartitionSnapshotStorageFactory(PartitionSnapshotStorage snapshotStorage) {
        this.snapshotStorage = snapshotStorage;
    }

    @Override
    public SnapshotStorage createSnapshotStorage(String uri, RaftOptions raftOptions) {
        return new PartitionSnapshotStorageAdapter(snapshotStorage, uri);
    }

    private static class PartitionSnapshotStorageAdapter implements SnapshotStorage {
        private final PartitionSnapshotStorage snapshotStorage;

        /** Flag indicating that startup snapshot has been opened. */
        private final AtomicBoolean startupSnapshotOpened = new AtomicBoolean();

        private final String snapshotUri;

        PartitionSnapshotStorageAdapter(PartitionSnapshotStorage snapshotStorage, String snapshotUri) {
            this.snapshotStorage = snapshotStorage;
            this.snapshotUri = snapshotUri;
        }

        @Override
        public boolean init(Void opts) {
            // No-op.
            return true;
        }

        @Override
        public void shutdown() {
            // No-op.
        }

        @Override
        @Nullable
        public SnapshotReader open() {
            if (startupSnapshotOpened.compareAndSet(false, true)) {
                SnapshotMeta startupSnapshotMeta = snapshotStorage.readStartupSnapshotMeta();

                if (startupSnapshotMeta == null) {
                    // The storage is empty, let's behave how JRaft does: return null, avoiding an attempt to load a snapshot
                    // when it's not there.
                    return null;
                }

                return new StartupPartitionSnapshotReader(startupSnapshotMeta, snapshotUri);
            }

            return snapshotStorage.startOutgoingSnapshot();
        }

        @Override
        public SnapshotCopier startToCopyFrom(String uri, SnapshotCopierOptions opts) {
            return snapshotStorage.startIncomingSnapshot(uri);
        }

        @Override
        public SnapshotWriter create() {
            return new PartitionSnapshotWriter(snapshotUri);
        }

        @Override
        public void setSnapshotThrottle(SnapshotThrottle snapshotThrottle) {
            // No-op.
        }

        @Override
        public SnapshotReader copyFrom(String uri, SnapshotCopierOptions opts) {
            throw new UnsupportedOperationException("Synchronous snapshot copy is not supported.");
        }

        @Override
        public boolean setFilterBeforeCopyRemote() {
            // Option is not supported.
            return false;
        }
    }
}
