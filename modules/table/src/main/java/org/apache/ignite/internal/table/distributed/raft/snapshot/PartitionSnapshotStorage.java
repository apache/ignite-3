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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.option.SnapshotCopierOptions;
import org.apache.ignite.raft.jraft.storage.SnapshotStorage;
import org.apache.ignite.raft.jraft.storage.SnapshotThrottle;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotCopier;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotWriter;

class PartitionSnapshotStorage implements SnapshotStorage {
    final String snapshotUri;

    final RaftOptions raftOptions;

    final MvPartitionStorage partitionStorage;

    final SnapshotMeta snapshotMeta;

    SnapshotThrottle snapshotThrottle;

    public PartitionSnapshotStorage(
            String snapshotUri,
            RaftOptions raftOptions,
            MvPartitionStorage partitionStorage,
            SnapshotMeta snapshotMeta
    ) {
        this.snapshotUri = snapshotUri;
        this.raftOptions = raftOptions;
        this.partitionStorage = partitionStorage;
        this.snapshotMeta = snapshotMeta;
    }

    /** {@inheritDoc} */
    @Override
    public boolean init(Void opts) {
        try {
            Files.createDirectories(Path.of(snapshotUri));

            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /** {@inheritDoc} */
    @Override
    public void shutdown() {
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
