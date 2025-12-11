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

package org.apache.ignite.internal.partition.replicator.raft.snapshot.outgoing;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.PartitionSnapshotStorage;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.SnapshotUri;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;

/**
 * {@link SnapshotReader} implementation for reading local rebalance snapshot.
 */
public class OutgoingSnapshotReader extends SnapshotReader {
    private static final IgniteLogger LOG = Loggers.forClass(OutgoingSnapshotReader.class);

    /** Snapshot id. */
    private final UUID id;

    /** Snapshot storage. */
    private final PartitionSnapshotStorage snapshotStorage;

    private final OutgoingSnapshot snapshot;

    /**
     * Constructor.
     */
    public OutgoingSnapshotReader(UUID snapshotId, PartitionSnapshotStorage snapshotStorage, OutgoingSnapshotMetricsSource metricSource) {
        this.snapshotStorage = snapshotStorage;

        id = snapshotId;

        snapshot = new OutgoingSnapshot(
                id,
                snapshotStorage.partitionKey(),
                snapshotStorage.partitionsByTableId(),
                snapshotStorage.txState(),
                snapshotStorage.catalogService(),
                metricSource
        );

        LOG.info("Starting snapshot reader [{}, snapshotId={}]", createPartitionInfo(), id);

        snapshotStorage.outgoingSnapshotsManager().startOutgoingSnapshot(id, snapshot);
    }

    @Override
    public SnapshotMeta load() {
        return snapshot.meta();
    }

    @Override
    public String generateURIForCopy() {
        String localNodeName = snapshotStorage.topologyService().localMember().name();

        return SnapshotUri.toStringUri(id, localNodeName);
    }

    @Override
    public void close() throws IOException {
        LOG.info("Closing snapshot reader [{}, snapshotId={}]", createPartitionInfo(), id);

        snapshotStorage.outgoingSnapshotsManager().finishOutgoingSnapshot(id);
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
    public String getPath() {
        throw new UnsupportedOperationException("No path for the rebalance snapshot");
    }

    @Override
    public Set<String> listFiles() {
        // No files in the snapshot.
        return Set.of();
    }

    @Override
    public Message getFileMeta(String fileName) {
        throw new UnsupportedOperationException("No files in the snapshot");
    }

    private String createPartitionInfo() {
        return snapshotStorage.partitionKey().toString();
    }
}
