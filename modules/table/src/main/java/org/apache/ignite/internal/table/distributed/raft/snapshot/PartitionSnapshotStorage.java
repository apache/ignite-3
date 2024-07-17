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

package org.apache.ignite.internal.table.distributed.raft.snapshot;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.table.distributed.raft.snapshot.incoming.IncomingSnapshotCopier;
import org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.OutgoingSnapshotReader;
import org.apache.ignite.internal.table.distributed.raft.snapshot.outgoing.OutgoingSnapshotsManager;
import org.apache.ignite.internal.table.distributed.raft.snapshot.startup.StartupPartitionSnapshotReader;
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
public class PartitionSnapshotStorage implements SnapshotStorage {
    /** Default number of milliseconds that the follower is allowed to try to catch up the required catalog version. */
    private static final int DEFAULT_WAIT_FOR_METADATA_CATCHUP_MS = 3000;

    /** Topology service. */
    private final TopologyService topologyService;

    /** Snapshot manager. */
    private final OutgoingSnapshotsManager outgoingSnapshotsManager;

    /** Snapshot URI. Points to a snapshot folder. Never created on physical storage. */
    private final String snapshotUri;

    /** Raft options. */
    private final RaftOptions raftOptions;

    /** Instance of partition. */
    private final PartitionAccess partition;

    private final CatalogService catalogService;

    /**
     *  Snapshot meta, constructed from the storage data and raft group configuration at startup.
     *  {@code null} if the storage is empty.
     */
    @Nullable
    private final SnapshotMeta startupSnapshotMeta;

    /** Incoming snapshots executor. */
    private final Executor incomingSnapshotsExecutor;

    private final long waitForMetadataCatchupMs;

    /** Snapshot throttle instance. */
    @Nullable
    private SnapshotThrottle snapshotThrottle;

    /** Flag indicating that startup snapshot has been opened. */
    private final AtomicBoolean startupSnapshotOpened = new AtomicBoolean();

    /**
     * Constructor.
     *
     * @param topologyService Topology service.
     * @param outgoingSnapshotsManager Outgoing snapshot manager.
     * @param snapshotUri Snapshot URI.
     * @param raftOptions RAFT options.
     * @param partition Partition.
     * @param catalogService Catalog service.
     * @param startupSnapshotMeta Snapshot meta at startup. {@code null} if the storage is empty.
     * @param incomingSnapshotsExecutor Incoming snapshots executor.
     */
    public PartitionSnapshotStorage(
            TopologyService topologyService,
            OutgoingSnapshotsManager outgoingSnapshotsManager,
            String snapshotUri,
            RaftOptions raftOptions,
            PartitionAccess partition,
            CatalogService catalogService,
            @Nullable SnapshotMeta startupSnapshotMeta,
            Executor incomingSnapshotsExecutor
    ) {
        this(
                topologyService,
                outgoingSnapshotsManager,
                snapshotUri,
                raftOptions,
                partition,
                catalogService,
                startupSnapshotMeta,
                incomingSnapshotsExecutor,
                DEFAULT_WAIT_FOR_METADATA_CATCHUP_MS
        );
    }

    /**
     * Constructor.
     *
     * @param topologyService Topology service.
     * @param outgoingSnapshotsManager Outgoing snapshot manager.
     * @param snapshotUri Snapshot URI.
     * @param raftOptions RAFT options.
     * @param partition Partition.
     * @param catalogService Catalog service.
     * @param startupSnapshotMeta Snapshot meta at startup. {@code null} if the storage is empty.
     * @param incomingSnapshotsExecutor Incoming snapshots executor.
     */
    public PartitionSnapshotStorage(
            TopologyService topologyService,
            OutgoingSnapshotsManager outgoingSnapshotsManager,
            String snapshotUri,
            RaftOptions raftOptions,
            PartitionAccess partition,
            CatalogService catalogService,
            @Nullable SnapshotMeta startupSnapshotMeta,
            Executor incomingSnapshotsExecutor,
            long waitForMetadataCatchupMs
    ) {
        this.topologyService = topologyService;
        this.outgoingSnapshotsManager = outgoingSnapshotsManager;
        this.snapshotUri = snapshotUri;
        this.raftOptions = raftOptions;
        this.partition = partition;
        this.catalogService = catalogService;
        this.startupSnapshotMeta = startupSnapshotMeta;
        this.incomingSnapshotsExecutor = incomingSnapshotsExecutor;
        this.waitForMetadataCatchupMs = waitForMetadataCatchupMs;
    }

    /**
     * Returns a topology service.
     */
    public TopologyService topologyService() {
        return topologyService;
    }

    /**
     * Returns an outgoing snapshots manager.
     */
    public OutgoingSnapshotsManager outgoingSnapshotsManager() {
        return outgoingSnapshotsManager;
    }

    /**
     * Returns a snapshot URI. Points to a snapshot folder. Never created on physical storage.
     */
    public String snapshotUri() {
        return snapshotUri;
    }

    /**
     * Returns raft options.
     */
    public RaftOptions raftOptions() {
        return raftOptions;
    }

    /**
     * Returns a partition.
     */
    public PartitionAccess partition() {
        return partition;
    }

    /**
     * Returns catalog service.
     */
    public CatalogService catalogService() {
        return catalogService;
    }

    /**
     * Returns a snapshot meta, constructed from the storage data and raft group configuration at startup.
     */
    public SnapshotMeta startupSnapshotMeta() {
        if (startupSnapshotMeta == null) {
            throw new IllegalStateException("Storage is empty, so startup snapshot should not be read");
        }

        return startupSnapshotMeta;
    }

    /**
     * Returns a snapshot throttle instance.
     */
    public @Nullable SnapshotThrottle snapshotThrottle() {
        return snapshotThrottle;
    }

    /**
     * Returns the incoming snapshots executor.
     */
    public Executor getIncomingSnapshotsExecutor() {
        return incomingSnapshotsExecutor;
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
    public boolean setFilterBeforeCopyRemote() {
        // Option is not supported.
        return false;
    }

    @Override
    public SnapshotWriter create() {
        return new PartitionSnapshotWriter(this);
    }

    @Override
    @Nullable
    public SnapshotReader open() {
        if (startupSnapshotOpened.compareAndSet(false, true)) {
            if (startupSnapshotMeta == null) {
                // The storage is empty, let's behave how JRaft does: return null, avoiding an attempt to load a snapshot
                // when it's not there.
                return null;
            }

            return new StartupPartitionSnapshotReader(this);
        }

        return new OutgoingSnapshotReader(this);
    }

    @Override
    public SnapshotReader copyFrom(String uri, SnapshotCopierOptions opts) {
        throw new UnsupportedOperationException("Synchronous snapshot copy is not supported.");
    }

    @Override
    public SnapshotCopier startToCopyFrom(String uri, SnapshotCopierOptions opts) {
        SnapshotUri snapshotUri = SnapshotUri.fromStringUri(uri);

        IncomingSnapshotCopier copier = new IncomingSnapshotCopier(this, snapshotUri, waitForMetadataCatchupMs);

        copier.start();

        return copier;
    }

    @Override
    public void setSnapshotThrottle(SnapshotThrottle snapshotThrottle) {
        this.snapshotThrottle = snapshotThrottle;
    }
}
