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

package org.apache.ignite.internal.partition.replicator.raft.snapshot.metrics;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;
import org.apache.ignite.internal.metrics.AbstractMetricSource;
import org.apache.ignite.internal.metrics.IntGauge;
import org.apache.ignite.internal.metrics.Metric;
import org.apache.ignite.internal.partition.replicator.raft.snapshot.metrics.RaftSnapshotsMetricsSource.Holder;

/**
 * Metric source that exposes counters related to Raft snapshots lifecycle for partition replicator.
 *
 * <p>The source is registered under the name {@code raft.snapshots}. It maintains the number of currently
 * running incoming and outgoing snapshots and per-phase counters for the installation of incoming snapshots.
 * These counters are intended to help understand where time is spent during snapshot installation and
 * whether there are any bottlenecks (for example, waiting for catalog, loading multi-versioned data, etc.).
 */
public class RaftSnapshotsMetricsSource extends AbstractMetricSource<Holder> {
    private final AtomicInteger incomingSnapshotsCounter = new AtomicInteger();

    private final AtomicInteger snapshotsLoadingMetaCounter = new AtomicInteger();

    private final AtomicInteger snapshotsWaitingCatalogCounter = new AtomicInteger();

    private final AtomicInteger snapshotsPreparingStoragesCounter = new AtomicInteger();

    private final AtomicInteger snapshotsPreparingIndexForBuildCounter = new AtomicInteger();

    private final AtomicInteger snapshotsLoadingMvDataCounter = new AtomicInteger();

    private final AtomicInteger snapshotsLoadingTxMetaCounter = new AtomicInteger();

    private final AtomicInteger outgoingSnapshotsCounter = new AtomicInteger();

    /**
     * Creates a new metric source with the name {@code raft.snapshots}.
     */
    public RaftSnapshotsMetricsSource() {
        super("raft.snapshots");
    }

    @Override
    protected Holder createHolder() {
        return new Holder(
                incomingSnapshotsCounter::get,
                snapshotsLoadingMetaCounter::get,
                snapshotsWaitingCatalogCounter::get,
                snapshotsPreparingStoragesCounter::get,
                snapshotsPreparingIndexForBuildCounter::get,
                snapshotsLoadingMvDataCounter::get,
                snapshotsLoadingTxMetaCounter::get,
                outgoingSnapshotsCounter::get
        );
    }

    /**
     * Marks the start of an incoming snapshot installation.
     * Increments the {@code TotalIncomingSnapshots} metric.
     */
    public void onSnapshotInstallationStart() {
        incomingSnapshotsCounter.incrementAndGet();
    }

    /**
     * Marks the end of an incoming snapshot installation.
     * Decrements the {@code TotalIncomingSnapshots} metric.
     */
    public void onSnapshotInstallationEnd() {
        incomingSnapshotsCounter.decrementAndGet();
    }

    /**
     * Marks the beginning of the "load snapshot metadata" phase during incoming snapshot installation.
     * Increments the {@code IncomingSnapshotsLoadingMeta} metric.
     */
    public void onLoadSnapshotMetaPhaseStart() {
        snapshotsLoadingMetaCounter.incrementAndGet();
    }

    /**
     * Marks the end of the "load snapshot metadata" phase during incoming snapshot installation.
     * Decrements the {@code IncomingSnapshotsLoadingMeta} metric.
     */
    public void onLoadSnapshotMetaPhaseEnd() {
        snapshotsLoadingMetaCounter.decrementAndGet();
    }

    /**
     * Marks the beginning of the phase where the node waits for catalog to be ready/apply updates
     * for the incoming snapshot installation.
     * Increments the {@code IncomingSnapshotsWaitingCatalog} metric.
     */
    public void onWaitingCatalogPhaseStart() {
        snapshotsWaitingCatalogCounter.incrementAndGet();
    }

    /**
     * Marks the end of the "waiting for catalog" phase during incoming snapshot installation.
     * Decrements the {@code IncomingSnapshotsWaitingCatalog} metric.
     */
    public void onWaitingCatalogPhaseEnd() {
        snapshotsWaitingCatalogCounter.decrementAndGet();
    }

    /**
     * Marks the beginning of the storage preparation phase for incoming snapshot installation.
     * Increments the {@code IncomingSnapshotsPreparingStorages} metric.
     */
    public void onPreparingStoragePhaseStart() {
        snapshotsPreparingStoragesCounter.incrementAndGet();
    }

    /**
     * Marks the end of the storage preparation phase for incoming snapshot installation.
     * Decrements the {@code IncomingSnapshotsPreparingStorages} metric.
     */
    public void onPreparingStoragePhaseEnd() {
        snapshotsPreparingStoragesCounter.decrementAndGet();
    }

    /**
     * Marks the beginning of the phase where MV (multi-version) data is loaded.
     * Increments the {@code IncomingSnapshotsLoadingMvData} metric.
     */
    public void onLoadMvDataPhaseStart() {
        snapshotsLoadingMvDataCounter.incrementAndGet();
    }

    /**
     * Marks the end of the phase where MV (multi-version) data is loaded.
     * Decrements the {@code IncomingSnapshotsLoadingMvData} metric.
     */
    public void onLoadMvDataPhaseEnd() {
        snapshotsLoadingMvDataCounter.decrementAndGet();
    }

    /**
     * Marks the beginning of the phase where transaction metadata is loaded from the snapshot.
     * Increments the {@code IncomingSnapshotsLoadingTxMeta} metric.
     */
    public void onLoadTxMetasPhaseStart() {
        snapshotsLoadingTxMetaCounter.incrementAndGet();
    }

    /**
     * Marks the end of the phase where transaction metadata is loaded from the snapshot.
     * Decrements the {@code IncomingSnapshotsLoadingTxMeta} metric.
     */
    public void onLoadTxMetasPhaseEnd() {
        snapshotsLoadingTxMetaCounter.decrementAndGet();
    }

    /**
     * Marks the beginning of preparing indexes for build as part of incoming snapshot installation.
     * Increments the {@code IncomingSnapshotsPreparingIndexForBuild} metric.
     */
    public void onSetRowIdToBuildPhaseStart() {
        snapshotsPreparingIndexForBuildCounter.incrementAndGet();
    }

    /**
     * Marks the end of preparing indexes for build as part of incoming snapshot installation.
     * Decrements the {@code IncomingSnapshotsPreparingIndexForBuild} metric.
     */
    public void onSetRowIdToBuildPhaseEnd() {
        snapshotsPreparingIndexForBuildCounter.decrementAndGet();
    }

    /**
     * Marks the start of an outgoing snapshot creation/streaming.
     * Increments the {@code TotalOutgoingSnapshots} metric.
     */
    public void onOutgoingSnapshotStart() {
        outgoingSnapshotsCounter.incrementAndGet();
    }

    /**
     * Marks the end of an outgoing snapshot creation/streaming.
     * Decrements the {@code TotalOutgoingSnapshots} metric.
     */
    public void onOutgoingSnapshotEnd() {
        outgoingSnapshotsCounter.decrementAndGet();
    }

    /**
     * Container of metrics exposed by {@link RaftSnapshotsMetricsSource}.
     */
    public static class Holder implements AbstractMetricSource.Holder<Holder> {
        private final IntGauge incomingSnapshots;

        private final IntGauge snapshotsLoadingMeta;

        private final IntGauge snapshotsWaitingCatalog;

        private final IntGauge snapshotsPreparingStorages;

        private final IntGauge snapshotsPreparingIndexForBuild;

        private final IntGauge snapshotsLoadingMvData;

        private final IntGauge snapshotsLoadingTxMeta;

        private final IntGauge outgoingSnapshots;

        private Holder(
                IntSupplier incomingSnapshotsSupplier,
                IntSupplier snapshotsLoadingMetaSupplier,
                IntSupplier snapshotsWaitingCatalogSupplier,
                IntSupplier snapshotsPreparingStoragesSupplier,
                IntSupplier snapshotsPreparingIndexForBuildSupplier,
                IntSupplier snapshotsLoadingMvDataSupplier,
                IntSupplier snapshotsLoadingTxMetaSupplier,
                IntSupplier outgoingSnapshotsSupplier
        ) {
            incomingSnapshots = new IntGauge(
                    "IncomingSnapshots",
                    "Incoming Raft snapshots in progress",
                    incomingSnapshotsSupplier
            );

            snapshotsLoadingMeta = new IntGauge(
                    "IncomingSnapshotsLoadingMeta",
                    "Incoming Raft snapshots loading metadata",
                    snapshotsLoadingMetaSupplier
            );

            snapshotsWaitingCatalog = new IntGauge(
                    "IncomingSnapshotsWaitingCatalog",
                    "Incoming Raft snapshots waiting for catalog",
                    snapshotsWaitingCatalogSupplier
            );

            snapshotsPreparingStorages = new IntGauge(
                    "IncomingSnapshotsPreparingStorages",
                    "Incoming Raft snapshots preparing storages",
                    snapshotsPreparingStoragesSupplier
            );

            snapshotsPreparingIndexForBuild = new IntGauge(
                    "IncomingSnapshotsPreparingIndexForBuild",
                    "Incoming Raft snapshots preparing indexes for build",
                    snapshotsPreparingIndexForBuildSupplier
            );

            snapshotsLoadingMvData = new IntGauge(
                    "IncomingSnapshotsLoadingMvData",
                    "Incoming Raft snapshots loading multi-versioned data",
                    snapshotsLoadingMvDataSupplier
            );

            snapshotsLoadingTxMeta = new IntGauge(
                    "IncomingSnapshotsLoadingTxMeta",
                    "Incoming Raft snapshots loading transaction metadata",
                    snapshotsLoadingTxMetaSupplier
            );

            outgoingSnapshots = new IntGauge(
                    "OutgoingSnapshots",
                    "Outgoing Raft snapshots in progress",
                    outgoingSnapshotsSupplier
            );
        }

        @Override
        public Iterable<Metric> metrics() {
            return List.of(
                    incomingSnapshots,
                    snapshotsLoadingMeta,
                    snapshotsWaitingCatalog,
                    snapshotsPreparingStorages,
                    snapshotsPreparingIndexForBuild,
                    snapshotsLoadingMvData,
                    snapshotsLoadingTxMeta,
                    outgoingSnapshots
            );
        }
    }
}
