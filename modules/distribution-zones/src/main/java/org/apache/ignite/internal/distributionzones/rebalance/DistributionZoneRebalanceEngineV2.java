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
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.ZONE_ALTER;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.filterDataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.parseDataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.extractZoneIdDataNodes;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.triggerZonePartitionsRebalance;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
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
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Zone rebalance manager. It listens to the changes in the distribution zones data nodes and replicas and triggers rebalance
 * for the corresponding partitions. By triggering rebalance, it updates the pending assignments in the metastore.
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
     * Starts the rebalance engine by registering corresponding meta storage and catalog listeners.
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

            CompletableFuture<Long> recoveryFinishFuture = metaStorageManager.recoveryFinishedFuture();

            // At the moment of the start of this manager, it is guaranteed that Meta Storage has been recovered.
            assert recoveryFinishFuture.isDone();

            long recoveryRevision = recoveryFinishFuture.join();

            return rebalanceTriggersRecovery(recoveryRevision);
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
                        // The zone was removed so data nodes were removed too.
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

                    return triggerZonePartitionsRebalance(
                            zoneDescriptor,
                            filteredDataNodes,
                            evt.entryEvent().newEntry().revision(),
                            metaStorageManager
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
        return recalculateAssignmentsAndTriggerZonePartitionsRebalance(
                parameters.zoneDescriptor(),
                parameters.causalityToken(),
                parameters.catalogVersion()
        );
    }

    /**
     * Calculate data nodes from the distribution zone for {@code zoneDescriptor} and {@code causalityToken} and trigger zones partitions
     * rebalance. For more details see
     * {@link ZoneRebalanceUtil#triggerZonePartitionsRebalance(CatalogZoneDescriptor, Set, long, MetaStorageManager)}
     *
     * @param zoneDescriptor Zone descriptor.
     * @param causalityToken Causality token.
     * @param catalogVersion Catalog version.
     * @return The future, which completes when the all metastore updates done.
     */
    private CompletableFuture<Void> recalculateAssignmentsAndTriggerZonePartitionsRebalance(
            CatalogZoneDescriptor zoneDescriptor,
            long causalityToken,
            int catalogVersion
    ) {
        return distributionZoneManager.dataNodes(causalityToken, catalogVersion, zoneDescriptor.id())
                .thenCompose(dataNodes -> IgniteUtils.inBusyLockAsync(busyLock, () -> {
                    if (dataNodes.isEmpty()) {
                        return nullCompletedFuture();
                    }

                    return triggerZonePartitionsRebalance(
                            zoneDescriptor,
                            dataNodes,
                            causalityToken,
                            metaStorageManager
                    );
                }));
    }

    /**
     * Run the update of rebalance metastore's state.
     *
     * @param recoveryRevision Recovery revision.
     */
    // TODO: https://issues.apache.org/jira/browse/IGNITE-21058 At the moment this method produce many metastore multi-invokes
    // TODO: which can be avoided by the local logic, which mirror the logic of metastore invokes.
    // TODO: And then run the remote invoke, only if needed.
    private CompletableFuture<Void> rebalanceTriggersRecovery(long recoveryRevision) {
        if (recoveryRevision > 0) {
            List<CompletableFuture<Void>> zonesRecoveryFutures = catalogService.zones(catalogService.latestCatalogVersion())
                    .stream()
                    .map(zoneDesc ->
                            recalculateAssignmentsAndTriggerZonePartitionsRebalance(
                                    zoneDesc,
                                    recoveryRevision,
                                    catalogService.latestCatalogVersion()
                            )
                    )
                    .collect(Collectors.toUnmodifiableList());

            return allOf(zonesRecoveryFutures.toArray(new CompletableFuture[0]));
        } else {
            return completedFuture(null);
        }
    }
}
