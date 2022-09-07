/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.table.distributed.raft.snapshot;

import org.apache.ignite.internal.storage.MvPartitionStorage;
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
 * Snapshot storage for {@link MvPartitionStorage}.
 *
 * @see PartitionSnapshotStorageFactory
 */
class PartitionSnapshotStorage implements SnapshotStorage {
    /** Snapshot URI. Points to a snapshot folder. Never created on physical storage. */
    final String snapshotUri;

    /** Raft options. */
    final RaftOptions raftOptions;

    /** Instance of partition. */
    final PartitionAccess partition;

    /** Snapshot meta, constructed from the storage data and reaft group configuration. */
    final SnapshotMeta snapshotMeta;

    /** Snapshot throttle instance. */
    @Nullable SnapshotThrottle snapshotThrottle;

    /**
     * Constructor.
     *
     * @param snapshotUri Snapshot URI.
     * @param raftOptions RAFT options.
     * @param partition Partition.
     * @param snapshotMeta Snapshot meta.
     */
    public PartitionSnapshotStorage(
            String snapshotUri,
            RaftOptions raftOptions,
            PartitionAccess partition,
            SnapshotMeta snapshotMeta
    ) {
        this.snapshotUri = snapshotUri;
        this.raftOptions = raftOptions;
        this.partition = partition;
        this.snapshotMeta = snapshotMeta;
    }

    /** {@inheritDoc} */
    @Override
    public boolean init(Void opts) {
        // No-op.
        return true;
    }

    /** {@inheritDoc} */
    @Override
    public void shutdown() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override
    public boolean setFilterBeforeCopyRemote() {
        // Option is not supported.
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public SnapshotWriter create() {
        return new PartitionSnapshotWriter(this);
    }

    /** {@inheritDoc} */
    @Override
    public SnapshotReader open() {
        return new InitPartitionSnapshotReader(this);
    }

    /** {@inheritDoc} */
    @Override
    public SnapshotReader copyFrom(String uri, SnapshotCopierOptions opts) {
        //TODO IGNITE-17083
        throw new UnsupportedOperationException("Not implemented yet: https://issues.apache.org/jira/browse/IGNITE-17083");
    }

    /** {@inheritDoc} */
    @Override
    public SnapshotCopier startToCopyFrom(String uri, SnapshotCopierOptions opts) {
        //TODO IGNITE-17083
        throw new UnsupportedOperationException("Not implemented yet: https://issues.apache.org/jira/browse/IGNITE-17083");
    }

    /** {@inheritDoc} */
    @Override
    public void setSnapshotThrottle(SnapshotThrottle snapshotThrottle) {
        this.snapshotThrottle = snapshotThrottle;
    }
}
