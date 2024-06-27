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

package org.apache.ignite.internal.distributionzones.rebalance;

import static java.util.concurrent.CompletableFuture.allOf;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.ZONE_ALTER;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.filterDataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.parseDataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.extractZoneIdDataNodes;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.affinity.AffinityUtils;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.AlterZoneEventParameters;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.Node;
import org.apache.ignite.internal.distributionzones.utils.CatalogAlterZoneEventListener;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Zone rebalance manager.
 * // TODO: https://issues.apache.org/jira/browse/IGNITE-22522 this class will replace DistributionZoneRebalanceEngine
 * // TODO: after switching to zone-based replication
 */
public class DistributionZoneRebalanceEngineV2 {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(DistributionZoneRebalanceEngineV2.class);

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** External busy lock. */
    private final IgniteSpinBusyLock busyLock;

    /** Meta Storage manager. */
    private final MetaStorageManager metaStorageManager;

    /** Distribution zones manager. */
    private final DistributionZoneManager distributionZoneManager;

    /** Meta storage listener for data nodes changes. */
    private final WatchListener dataNodesListener;

    /** Catalog service. */
    private final CatalogService catalogService;

    /**
     * Constructor.
     *
     * @param busyLock External busy lock.
     * @param metaStorageManager Meta Storage manager.
     * @param distributionZoneManager Distribution zones manager.
     * @param catalogService Catalog service.
     */
    public DistributionZoneRebalanceEngineV2(
            IgniteSpinBusyLock busyLock,
            MetaStorageManager metaStorageManager,
            DistributionZoneManager distributionZoneManager,
            CatalogManager catalogService
    ) {
        this.busyLock = busyLock;
        this.metaStorageManager = metaStorageManager;
        this.distributionZoneManager = distributionZoneManager;
        this.catalogService = catalogService;

        this.dataNodesListener = createDistributionZonesDataNodesListener();
    }

    /**
     * Starts the rebalance engine by registering corresponding meta storage and configuration listeners.
     */
    public CompletableFuture<Void> start() {
        return IgniteUtils.inBusyLockAsync(busyLock, () -> {
            catalogService.listen(ZONE_ALTER, new CatalogAlterZoneEventListener(catalogService) {
                @Override
                protected CompletableFuture<Void> onReplicasUpdate(AlterZoneEventParameters parameters, int oldReplicas) {
                    return onUpdateReplicas(parameters);
                }
            });

            // TODO: IGNITE-18694 - Recovery for the case when zones watch listener processed event but assignments were not updated.
            metaStorageManager.registerPrefixWatch(zoneDataNodesKey(), dataNodesListener);

            return nullCompletedFuture();
        });
    }

    /**
     * Stops the rebalance engine by unregistering meta storage watches.
     */
    public void stop() {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        metaStorageManager.unregisterWatch(dataNodesListener);
    }

    private WatchListener createDistributionZonesDataNodesListener() {
        return new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent evt) {
                return IgniteUtils.inBusyLockAsync(busyLock, () -> {
                    Set<Node> dataNodes = parseDataNodes(evt.entryEvent().newEntry().value());

                    if (dataNodes == null) {
                        // The zone was removed so data nodes was removed too.
                        return nullCompletedFuture();
                    }

                    int zoneId = extractZoneIdDataNodes(evt.entryEvent().newEntry().key());

                    // It is safe to get the latest version of the catalog as we are in the metastore thread.
                    int catalogVersion = catalogService.latestCatalogVersion();

                    CatalogZoneDescriptor zoneDescriptor = catalogService.zone(zoneId, catalogVersion);

                    if (zoneDescriptor == null) {
                        // Zone has been removed.
                        return nullCompletedFuture();
                    }

                    Set<String> filteredDataNodes = filterDataNodes(
                            dataNodes,
                            zoneDescriptor,
                            distributionZoneManager.nodesAttributes()
                    );

                    if (filteredDataNodes.isEmpty()) {
                        return nullCompletedFuture();
                    }

                    return triggerPartitionsRebalanceForZone(
                            evt.entryEvent().newEntry().revision(),
                            zoneDescriptor,
                            filteredDataNodes
                    );
                });
            }

            @Override
            public void onError(Throwable e) {
                LOG.warn("Unable to process data nodes event", e);
            }
        };
    }

    private CompletableFuture<Void> onUpdateReplicas(AlterZoneEventParameters parameters) {
        return recalculateAssignmentsAndScheduleRebalance(
                parameters.zoneDescriptor(),
                parameters.causalityToken(),
                parameters.catalogVersion()
        );
    }

    /**
     * Recalculate assignments for zone partitions and schedule rebalance (by update rebalance metastore keys).
     *
     * @param zoneDescriptor Zone descriptor.
     * @param causalityToken Causality token.
     * @param catalogVersion Catalog version.
     * @return The future, which completes when the all metastore updates done.
     */
    private CompletableFuture<Void> recalculateAssignmentsAndScheduleRebalance(
            CatalogZoneDescriptor zoneDescriptor,
            long causalityToken,
            int catalogVersion
    ) {
        return distributionZoneManager.dataNodes(causalityToken, catalogVersion, zoneDescriptor.id())
                .thenCompose(dataNodes -> {
                    if (dataNodes.isEmpty()) {
                        return nullCompletedFuture();
                    }

                    return triggerPartitionsRebalanceForZone(
                            causalityToken,
                            zoneDescriptor,
                            dataNodes
                    );
                });
    }

    private CompletableFuture<Void> triggerPartitionsRebalanceForZone(
            long revision,
            CatalogZoneDescriptor zoneDescriptor,
            Set<String> dataNodes
    ) {
        CompletableFuture<?>[] partitionFutures = ZoneRebalanceUtil.triggerZonePartitionsRebalance(
                zoneDescriptor,
                dataNodes,
                revision,
                metaStorageManager
        );

        // This set is used to deduplicate exceptions (if there is an exception from upstream, for instance,
        // when reading from MetaStorage, it will be encountered by every partition future) to avoid noise
        // in the logs.
        Set<Throwable> unwrappedCauses = ConcurrentHashMap.newKeySet();

        for (int partId = 0; partId < partitionFutures.length; partId++) {
            int finalPartId = partId;

            partitionFutures[partId].exceptionally(e -> {
                Throwable cause = ExceptionUtils.unwrapCause(e);

                if (unwrappedCauses.add(cause)) {
                    // The exception is specific to this partition.
                    LOG.error(
                            "Exception on updating assignments for [zone={}, partition={}]",
                            e,
                            zoneInfo(zoneDescriptor), finalPartId
                    );
                } else {
                    // The exception is from upstream and not specific for this partition, so don't log the partition index.
                    LOG.error(
                            "Exception on updating assignments for [zone={}]",
                            e,
                            zoneInfo(zoneDescriptor)
                    );
                }

                return null;
            });
        }

        return allOf(partitionFutures);
    }

    private static String zoneInfo(CatalogZoneDescriptor zoneDescriptor) {
        return zoneDescriptor.id() + "/" + zoneDescriptor.name();
    }

    static CompletableFuture<Set<Assignment>> calculateZoneAssignments(
            ZonePartitionId zonePartitionId,
            CatalogService catalogService,
            DistributionZoneManager distributionZoneManager
    ) {
        int catalogVersion = catalogService.latestCatalogVersion();

        CatalogZoneDescriptor zoneDescriptor = catalogService.zone(zonePartitionId.zoneId(), catalogVersion);

        int zoneId = zonePartitionId.zoneId();

        return distributionZoneManager.dataNodes(
                zoneDescriptor.updateToken(),
                catalogVersion,
                zoneId
        ).thenApply(dataNodes ->
                AffinityUtils.calculateAssignmentForPartition(
                        dataNodes,
                        zonePartitionId.partitionId(),
                        zoneDescriptor.replicas()
                )
        );
    }
}
