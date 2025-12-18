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

package org.apache.ignite.internal.partition.replicator.raft.snapshot.incoming;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import org.apache.ignite.internal.metrics.StopWatchTimer;

/**
 * Statistical information related to {@link IncomingSnapshotCopier}.
 *
 * <p>Collects counters and phase durations (in milliseconds) for the incoming snapshot installation process.</p>
 */
class IncomingSnapshotStats {
    private long totalMvRows;

    private int totalMvBatches;

    private long totalTxMetas;

    private int totalTxMetasBatches;

    private final StopWatchTimer loadSnapshotMetaTimer = new StopWatchTimer();

    private final StopWatchTimer waitingCatalogTimer = new StopWatchTimer();

    private final StopWatchTimer preparingStoragesTimer = new StopWatchTimer();

    private final StopWatchTimer loadMvDataTimer = new StopWatchTimer();

    private final StopWatchTimer loadTxDataTimer = new StopWatchTimer();

    private final StopWatchTimer setRowIdToBuildIndexTimer = new StopWatchTimer();

    private final StopWatchTimer totalSnapshotInstallationTimer = new StopWatchTimer();

    void onMvBatchProcessing(long rows) {
        totalMvRows += rows;

        totalMvBatches++;
    }

    void onLoadMvDataPhaseStart() {
        loadMvDataTimer.start();
    }

    void onLoadMvDataPhaseEnd() {
        loadMvDataTimer.end();
    }

    long totalMvDataRows() {
        return totalMvRows;
    }

    long totalMvDataBatches() {
        return totalMvBatches;
    }

    long loadMvDataPhaseDuration() {
        return loadMvDataTimer.duration(MILLISECONDS);
    }

    void onTxMetasBatchProcessing(long metas) {
        totalTxMetas += metas;

        totalTxMetasBatches++;
    }

    void onLoadTxMetasPhaseStart() {
        loadTxDataTimer.start();
    }

    void onLoadTxMetasPhaseEnd() {
        loadTxDataTimer.end();
    }

    long totalTxMetas() {
        return totalTxMetas;
    }

    long totalTxMetasBatches() {
        return totalTxMetasBatches;
    }

    long loadTxMetasPhaseDuration() {
        return loadTxDataTimer.duration(MILLISECONDS);
    }

    void onSnapshotInstallationStart() {
        totalSnapshotInstallationTimer.start();
    }

    void onSnapshotInstallationEnd() {
        totalSnapshotInstallationTimer.end();
    }

    long totalSnapshotInstallationDuration() {
        return totalSnapshotInstallationTimer.duration(MILLISECONDS);
    }

    void onLoadSnapshotPhaseStart() {
        loadSnapshotMetaTimer.start();
    }

    void onLoadSnapshotPhaseEnd() {
        loadSnapshotMetaTimer.end();
    }

    long totalLoadSnapshotPhaseDuration() {
        return loadSnapshotMetaTimer.duration(MILLISECONDS);
    }

    void onWaitingCatalogPhaseStart() {
        waitingCatalogTimer.start();
    }

    void onWaitingCatalogPhaseEnd() {
        waitingCatalogTimer.end();
    }

    long totalWaitingCatalogPhaseDuration() {
        return waitingCatalogTimer.duration(MILLISECONDS);
    }

    void onPreparingStoragePhaseStart() {
        preparingStoragesTimer.start();
    }

    void onPreparingStoragePhaseEnd() {
        preparingStoragesTimer.end();
    }

    long totalPreparingStoragePhaseDuration() {
        return preparingStoragesTimer.duration(MILLISECONDS);
    }

    void onSetRowIdToBuildPhaseStart() {
        setRowIdToBuildIndexTimer.start();
    }

    void onSetRowIdToBuildPhaseEnd() {
        setRowIdToBuildIndexTimer.end();
    }

    long totalSetRowIdToBuildPhaseDuration() {
        return setRowIdToBuildIndexTimer.duration(MILLISECONDS);
    }
}
