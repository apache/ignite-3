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
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.DISTRIBUTION_ZONE_DATA_NODES_VALUE_PREFIX;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.filterDataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.findTablesByZoneId;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.parseDataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.extractZoneId;
import static org.apache.ignite.internal.lang.IgniteSystemProperties.getBoolean;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
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
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Zone rebalance manager.
 */
public class DistributionZoneRebalanceEngine {
    /** The logger. */
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

    /** Zone rebalance manager. */
    // TODO: https://issues.apache.org/jira/browse/IGNITE-22522 this class will replace DistributionZoneRebalanceEngine
    // TODO: after switching to zone-based replication
    private final DistributionZoneRebalanceEngineV2 distributionZoneRebalanceEngineV2;

    public static final String FEATURE_FLAG_NAME = "IGNITE_ZONE_BASED_REPLICATION";
    /* Feature flag for zone based collocation track */
    // TODO IGNITE-22115 remove it
    public static final boolean ENABLED = getBoolean(FEATURE_FLAG_NAME, false);

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
            CatalogManager catalogService
    ) {
        this.busyLock = busyLock;
        this.metaStorageManager = metaStorageManager;
        this.distributionZoneManager = distributionZoneManager;
        this.catalogService = catalogService;

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
            metaStorageManager.registerPrefixWatch(zoneDataNodesKey(), dataNodesListener);

            CompletableFuture<Long> recoveryFinishFuture = metaStorageManager.recoveryFinishedFuture();

            // At the moment of the start of this manager, it is guaranteed that Meta Storage has been recovered.
            assert recoveryFinishFuture.isDone();

            long recoveryRevision = recoveryFinishFuture.join();

            if (ENABLED) {
                return rebalanceTriggersRecovery(recoveryRevision, catalogVersion)
                        .thenCompose(v -> distributionZoneRebalanceEngineV2.startAsync());
            } else {
                return rebalanceTriggersRecovery(recoveryRevision, catalogVersion);
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
    private CompletableFuture<Void> rebalanceTriggersRecovery(long recoveryRevision, int catalogVersion) {
        if (recoveryRevision > 0) {
            List<CompletableFuture<Void>> zonesRecoveryFutures = catalogService.zones(catalogVersion)
                    .stream()
                    .map(zoneDesc ->
                            recalculateAssignmentsAndScheduleRebalance(
                                    zoneDesc,
                                    recoveryRevision,
                                    catalogVersion
                            )
                    )
                    .collect(Collectors.toUnmodifiableList());

            return allOf(zonesRecoveryFutures.toArray(new CompletableFuture[0]));
        } else {
            return completedFuture(null);
        }
    }

    /**
     * Stops the rebalance engine by unregistering meta storage watches.
     */
    public void stop() {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        if (ENABLED) {
            distributionZoneRebalanceEngineV2.stop();
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

                    int zoneId = extractZoneId(evt.entryEvent().newEntry().key(), DISTRIBUTION_ZONE_DATA_NODES_VALUE_PREFIX);

                    // It is safe to get the latest version of the catalog as we are in the metastore thread.
                    // TODO: IGNITE-22723 Potentially unsafe to use the latest catalog version, as the tables might not already present
                    //  in the catalog. Better to store this version when writing datanodes.
                    int catalogVersion = catalogService.latestCatalogVersion();

                    Catalog catalog = catalogService.catalog(catalogVersion);

                    long assignmentsTimestamp = catalog.time();

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

                    List<CatalogTableDescriptor> tableDescriptors = findTablesByZoneId(zoneId, catalogVersion, catalogService);

                    return triggerPartitionsRebalanceForAllTables(
                            evt.entryEvent().newEntry().revision(),
                            zoneDescriptor,
                            filteredDataNodes,
                            tableDescriptors,
                            assignmentsTimestamp
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
     * Recalculate assignments for table partitions of target zone and schedule rebalance (by update rebalance metastore keys).
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

                    List<CatalogTableDescriptor> tableDescriptors = findTablesByZoneId(zoneDescriptor.id(), catalogVersion, catalogService);

                    Catalog catalog = catalogService.catalog(catalogVersion);

                    return triggerPartitionsRebalanceForAllTables(
                            causalityToken,
                            zoneDescriptor,
                            dataNodes,
                            tableDescriptors,
                            catalog.time()
                    );
                });
    }

    private CompletableFuture<Void> triggerPartitionsRebalanceForAllTables(
            long revision,
            CatalogZoneDescriptor zoneDescriptor,
            Set<String> dataNodes,
            List<CatalogTableDescriptor> tableDescriptors,
            long assignmentsTimestamp
    ) {
        List<CompletableFuture<?>> tableFutures = new ArrayList<>(tableDescriptors.size());

        for (CatalogTableDescriptor tableDescriptor : tableDescriptors) {
            CompletableFuture<?>[] partitionFutures = RebalanceUtil.triggerAllTablePartitionsRebalance(
                    tableDescriptor,
                    zoneDescriptor,
                    dataNodes,
                    revision,
                    metaStorageManager,
                    assignmentsTimestamp
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
                                "Exception on updating assignments for [table={}, partition={}]",
                                e,
                                tableInfo(tableDescriptor), finalPartId
                        );
                    } else {
                        // The exception is from upstream and not specific for this partition, so don't log the partition index.
                        LOG.error(
                                "Exception on updating assignments for [table={}]",
                                e,
                                tableInfo(tableDescriptor)
                        );
                    }

                    return null;
                });
            }

            tableFutures.add(allOf(partitionFutures));
        }

        return allOf(tableFutures.toArray(CompletableFuture[]::new));
    }

    private static String tableInfo(CatalogTableDescriptor tableDescriptor) {
        return tableDescriptor.id() + "/" + tableDescriptor.name();
    }
}
