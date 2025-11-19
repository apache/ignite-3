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
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.DISTRIBUTION_ZONE_DATA_NODES_HISTORY_PREFIX_BYTES;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.filterDataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.nodeNames;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.parseDataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesHistoryPrefix;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.extractZoneId;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.AlterZoneEventParameters;
import org.apache.ignite.internal.components.NodeProperties;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.NodeWithAttributes;
import org.apache.ignite.internal.distributionzones.utils.CatalogAlterZoneEventListener;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.Revisions;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.TestOnly;

/**
 * Zone rebalance manager.
 */
public class DistributionZoneRebalanceEngine {
    private static final IgniteLogger LOG = Loggers.forClass(DistributionZoneRebalanceEngine.class);

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

    private final NodeProperties nodeProperties;

    /** Zone rebalance manager. */
    // TODO: https://issues.apache.org/jira/browse/IGNITE-22522 this class will replace DistributionZoneRebalanceEngine
    // TODO: after switching to zone-based replication
    private final DistributionZoneRebalanceEngineV2 distributionZoneRebalanceEngineV2;

    /** Special flag to skip rebalance on node recovery for tests. */
    // TODO: IGNITE-24607 Remove it
    @TestOnly
    public static final String SKIP_REBALANCE_TRIGGERS_RECOVERY = "IGNITE_SKIP_REBALANCE_TRIGGERS_RECOVERY";

    /**
     * Constructor.
     *
     * @param busyLock External busy lock.
     * @param metaStorageManager Meta Storage manager.
     * @param distributionZoneManager Distribution zones manager.
     * @param catalogService Catalog service.
     */
    public DistributionZoneRebalanceEngine(
            IgniteSpinBusyLock busyLock,
            MetaStorageManager metaStorageManager,
            DistributionZoneManager distributionZoneManager,
            CatalogManager catalogService,
            NodeProperties nodeProperties
    ) {
        this.busyLock = busyLock;
        this.metaStorageManager = metaStorageManager;
        this.distributionZoneManager = distributionZoneManager;
        this.catalogService = catalogService;
        this.nodeProperties = nodeProperties;

        this.dataNodesListener = createDistributionZonesDataNodesListener();
        this.distributionZoneRebalanceEngineV2 = new DistributionZoneRebalanceEngineV2(
                busyLock,
                metaStorageManager,
                distributionZoneManager,
                catalogService
        );
    }

    /**
     * Starts the rebalance engine by registering corresponding meta storage and configuration listeners.
     */
    public CompletableFuture<Void> startAsync(int catalogVersion) {
        return IgniteUtils.inBusyLockAsync(busyLock, () -> {
            catalogService.listen(ZONE_ALTER, new CatalogAlterZoneEventListener(catalogService) {
                @Override
                protected CompletableFuture<Void> onReplicasUpdate(AlterZoneEventParameters parameters, int oldReplicas) {
                    return onUpdateReplicas(parameters);
                }
            });

            // TODO: IGNITE-18694 - Recovery for the case when zones watch listener processed event but assignments were not updated.
            metaStorageManager.registerPrefixWatch(zoneDataNodesHistoryPrefix(), dataNodesListener);

            CompletableFuture<Revisions> recoveryFinishFuture = metaStorageManager.recoveryFinishedFuture();

            // At the moment of the start of this manager, it is guaranteed that Meta Storage has been recovered.
            assert recoveryFinishFuture.isDone();

            long recoveryRevision = recoveryFinishFuture.join().revision();

            if (getBoolean(SKIP_REBALANCE_TRIGGERS_RECOVERY, false)) {
                return nullCompletedFuture();
            }

            if (nodeProperties.colocationEnabled()) {
                return recoveryRebalanceTrigger(recoveryRevision, catalogVersion)
                        .thenCompose(v -> distributionZoneRebalanceEngineV2.startAsync());
            } else {
                return recoveryRebalanceTrigger(recoveryRevision, catalogVersion);
            }
        });
    }

    /**
     * Run the update of rebalance metastore's state.
     *
     * @param recoveryRevision Recovery revision.
     */
    // TODO: https://issues.apache.org/jira/browse/IGNITE-21058 At the moment this method produce many metastore multi-invokes
    // TODO: which can be avoided by the local logic, which mirror the logic of metastore invokes.
    // TODO: And then run the remote invoke, only if needed.
    private CompletableFuture<Void> recoveryRebalanceTrigger(long recoveryRevision, int catalogVersion) {
        if (recoveryRevision > 0) {
            HybridTimestamp recoveryTimestamp = metaStorageManager.timestampByRevisionLocally(recoveryRevision);

            List<CompletableFuture<Void>> zonesRecoveryFutures = catalogService.catalog(catalogVersion).zones()
                    .stream()
                    .map(zoneDesc ->
                            recalculateAssignmentsAndScheduleRebalance(
                                    zoneDesc,
                                    recoveryRevision,
                                    recoveryTimestamp,
                                    catalogVersion
                            )
                    )
                    .collect(Collectors.toUnmodifiableList());

            return allOf(zonesRecoveryFutures.toArray(new CompletableFuture[0]));
        } else {
            return nullCompletedFuture();
        }
    }

    /**
     * Stops the rebalance engine by unregistering meta storage watches.
     */
    public void stop() {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        if (nodeProperties.colocationEnabled()) {
            distributionZoneRebalanceEngineV2.stop();
        }

        metaStorageManager.unregisterWatch(dataNodesListener);
    }

    private WatchListener createDistributionZonesDataNodesListener() {
        return evt -> IgniteUtils.inBusyLockAsync(busyLock, () -> {
            Set<NodeWithAttributes> dataNodes = parseDataNodes(evt.entryEvent().newEntry().value(), evt.timestamp());

            if (dataNodes == null) {
                // The zone was removed so data nodes was removed too.
                return nullCompletedFuture();
            }

            int zoneId = extractZoneId(evt.entryEvent().newEntry().key(), DISTRIBUTION_ZONE_DATA_NODES_HISTORY_PREFIX_BYTES);

            // It is safe to get the latest version of the catalog as we are in the metastore thread.
            // TODO: IGNITE-22723 Potentially unsafe to use the latest catalog version, as the tables might not already present
            //  in the catalog. Better to store this version when writing datanodes.
            int latestCatalogVersion = catalogService.latestCatalogVersion();
            Catalog catalog = catalogService.catalog(latestCatalogVersion);

            long assignmentsTimestamp = catalog.time();

            CatalogZoneDescriptor zoneDescriptor = catalog.zone(zoneId);

            if (zoneDescriptor == null) {
                // Zone has been removed.
                return nullCompletedFuture();
            }

            Set<String> filteredDataNodes = nodeNames(filterDataNodes(dataNodes, zoneDescriptor));

            if (LOG.isInfoEnabled()) {
                var matchedNodes = new ArrayList<NodeWithAttributes>();
                var filteredOutNodes = new ArrayList<NodeWithAttributes>();

                for (NodeWithAttributes dataNode : dataNodes) {
                    if (filteredDataNodes.contains(dataNode.nodeName())) {
                        matchedNodes.add(dataNode);
                    } else {
                        filteredOutNodes.add(dataNode);
                    }
                }

                if (!filteredOutNodes.isEmpty() && !filteredDataNodes.isEmpty()) {
                    LOG.info(
                            "Some data nodes were filtered out because they don't match zone's attributes:"
                                    + "\n\tzoneId={}\n\tfilter={}\n\tstorageProfiles={}'\n\tfilteredOutNodes={}\n\tremainingNodes={}",
                            zoneDescriptor.id(),
                            zoneDescriptor.filter(),
                            zoneDescriptor.storageProfiles(),
                            filteredOutNodes,
                            matchedNodes
                    );
                }
            }

            if (filteredDataNodes.isEmpty()) {
                LOG.info("Rebalance is not triggered because data nodes are empty [zoneId={}, filter={}, storageProfiles={}]",
                        zoneDescriptor.id(),
                        zoneDescriptor.filter(),
                        zoneDescriptor.storageProfiles().profiles()
                );

                return nullCompletedFuture();
            }

            return triggerPartitionsRebalanceForAllTables(
                    evt.entryEvent().newEntry().revision(),
                    evt.entryEvent().newEntry().timestamp(),
                    zoneDescriptor,
                    filteredDataNodes,
                    catalog.tables(zoneId),
                    assignmentsTimestamp
            );
        });
    }

    private CompletableFuture<Void> onUpdateReplicas(AlterZoneEventParameters parameters) {
        return recalculateAssignmentsAndScheduleRebalance(
                parameters.zoneDescriptor(),
                parameters.causalityToken(),
                parameters.zoneDescriptor().updateTimestamp(),
                parameters.catalogVersion()
        );
    }

    /**
     * Recalculate assignments for table partitions of target zone and schedule rebalance (by update rebalance metastore keys).
     *
     * @param zoneDescriptor Zone descriptor.
     * @param causalityToken Causality token.
     * @param timestamp Timestamp corresponding to the causality token.
     * @param catalogVersion Catalog version.
     * @return The future, which completes when the all metastore updates done.
     */
    private CompletableFuture<Void> recalculateAssignmentsAndScheduleRebalance(
            CatalogZoneDescriptor zoneDescriptor,
            long causalityToken,
            HybridTimestamp timestamp,
            int catalogVersion
    ) {
        return distributionZoneManager.dataNodes(catalogVersion, zoneDescriptor.id())
                .thenCompose(dataNodes -> {
                    if (dataNodes.isEmpty()) {
                        return nullCompletedFuture();
                    }

                    Catalog catalog = catalogService.catalog(catalogVersion);

                    return triggerPartitionsRebalanceForAllTables(
                            causalityToken,
                            timestamp,
                            zoneDescriptor,
                            dataNodes,
                            catalog.tables(zoneDescriptor.id()),
                            catalog.time()
                    );
                });
    }

    private CompletableFuture<Void> triggerPartitionsRebalanceForAllTables(
            long revision,
            HybridTimestamp timestamp,
            CatalogZoneDescriptor zoneDescriptor,
            Set<String> dataNodes,
            Collection<CatalogTableDescriptor> tableDescriptors,
            long assignmentsTimestamp
    ) {
        List<CompletableFuture<?>> tableFutures = new ArrayList<>(tableDescriptors.size());

        Set<String> aliveNodes = nodeNames(distributionZoneManager.logicalTopology(revision));

        for (CatalogTableDescriptor tableDescriptor : tableDescriptors) {
            tableFutures.add(RebalanceUtil.triggerAllTablePartitionsRebalance(
                    tableDescriptor,
                    zoneDescriptor,
                    dataNodes,
                    revision,
                    timestamp,
                    metaStorageManager,
                    assignmentsTimestamp,
                    aliveNodes
            ));
        }

        return allOf(tableFutures.toArray(CompletableFuture[]::new));
    }

}
