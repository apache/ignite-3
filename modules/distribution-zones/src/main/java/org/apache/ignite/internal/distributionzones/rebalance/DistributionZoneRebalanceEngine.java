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

import static java.util.Collections.newSetFromMap;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.ZONE_ALTER;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.extractZoneId;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.filterDataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.parseDataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.catalog.CatalogManager;
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
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.NodeStoppingException;

/**
 * Zone rebalance manager.
 */
public class DistributionZoneRebalanceEngine {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(DistributionZoneRebalanceEngine.class);

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock;

    /** Meta Storage manager. */
    private final MetaStorageManager metaStorageManager;

    /** Distribution zones manager. */
    private final DistributionZoneManager distributionZoneManager;

    /** Meta storage listener for data nodes changes. */
    private final WatchListener dataNodesListener;

    /** Catalog manager. */
    private final CatalogManager catalogManager;

    /**
     * The constructor.
     *
     * @param stopGuard Prevents double stopping of the component.
     * @param busyLock Busy lock to stop synchronously.
     * @param metaStorageManager Meta Storage manager.
     * @param distributionZoneManager Distribution zones manager.
     * @param catalogManager Catalog manager.
     */
    public DistributionZoneRebalanceEngine(
            AtomicBoolean stopGuard,
            IgniteSpinBusyLock busyLock,
            MetaStorageManager metaStorageManager,
            DistributionZoneManager distributionZoneManager,
            CatalogManager catalogManager
    ) {
        this.stopGuard = stopGuard;
        this.busyLock = busyLock;
        this.metaStorageManager = metaStorageManager;
        this.distributionZoneManager = distributionZoneManager;
        this.catalogManager = catalogManager;

        this.dataNodesListener = createDistributionZonesDataNodesListener();
    }

    /**
     * Starts the rebalance engine by registering corresponding meta storage and configuration listeners.
     */
    public void start() {
        IgniteUtils.inBusyLock(busyLock, () -> {
            catalogManager.listen(ZONE_ALTER, new CatalogAlterZoneEventListener(catalogManager) {
                @Override
                protected CompletableFuture<Void> onReplicasUpdate(AlterZoneEventParameters parameters, int oldReplicas) {
                    return onUpdateReplicas(parameters, oldReplicas);
                }
            });

            // TODO: IGNITE-18694 - Recovery for the case when zones watch listener processed event but assignments were not updated.
            metaStorageManager.registerPrefixWatch(zoneDataNodesKey(), dataNodesListener);
        });
    }

    /**
     * Stops the rebalance engine by unregistering meta storage watches.
     */
    public void stop() {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        metaStorageManager.unregisterWatch(dataNodesListener);
    }

    private WatchListener createDistributionZonesDataNodesListener() {
        return new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent evt) {
                if (!busyLock.enterBusy()) {
                    return failedFuture(new NodeStoppingException());
                }

                try {
                    Set<Node> dataNodes = parseDataNodes(evt.entryEvent().newEntry().value());

                    if (dataNodes == null) {
                        //The zone was removed so data nodes was removed too.
                        return completedFuture(null);
                    }

                    int zoneId = extractZoneId(evt.entryEvent().newEntry().key());

                    // It is safe to get the latest version of the catalog as we are in the metastore thread.
                    int catalogVersion = catalogManager.latestCatalogVersion();

                    CatalogZoneDescriptor zoneDescriptor = catalogManager.zone(zoneId, catalogVersion);

                    assert zoneDescriptor != null : zoneId;

                    Set<String> filteredDataNodes = filterDataNodes(
                            dataNodes,
                            zoneDescriptor.filter(),
                            distributionZoneManager.nodesAttributes()
                    );

                    if (filteredDataNodes.isEmpty()) {
                        return completedFuture(null);
                    }

                    for (CatalogTableDescriptor tableDescriptor : findTablesByZoneId(zoneId, catalogVersion)) {
                        CompletableFuture<?>[] partitionFutures = RebalanceUtil.triggerAllTablePartitionsRebalance(
                                tableDescriptor,
                                zoneDescriptor,
                                filteredDataNodes,
                                evt.entryEvent().newEntry().revision(),
                                metaStorageManager
                        );

                        // This set is used to deduplicate exceptions (if there is an exception from upstream, for instance,
                        // when reading from MetaStorage, it will be encountered by every partition future) to avoid noise
                        // in the logs.
                        Set<Throwable> exceptions = newSetFromMap(new ConcurrentHashMap<>());

                        for (int partId = 0; partId < partitionFutures.length; partId++) {
                            int finalPartId = partId;

                            partitionFutures[partId].exceptionally(e -> {
                                if (exceptions.add(e)) {
                                    // The exception is specific to this partition.
                                    LOG.error(
                                            "Exception on updating assignments for [table={}/{}, partition={}]", e,
                                            tableDescriptor.id(), tableDescriptor.name(), finalPartId
                                    );
                                } else {
                                    // The exception is from upstream and not specific for this partition, so don't log the partition index.
                                    LOG.error(
                                            "Exception on updating assignments for [table={}/{}]", e,
                                            tableDescriptor.id(), tableDescriptor.name()
                                    );
                                }

                                return null;
                            });
                        }
                    }

                    return completedFuture(null);
                } catch (Throwable t) {
                    return failedFuture(t);
                } finally {
                    busyLock.leaveBusy();
                }
            }

            @Override
            public void onError(Throwable e) {
                LOG.warn("Unable to process data nodes event", e);
            }
        };
    }

    private CompletableFuture<Void> onUpdateReplicas(AlterZoneEventParameters parameters, int oldReplicas) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            int zoneId = parameters.zoneDescriptor().id();
            long causalityToken = parameters.causalityToken();

            return distributionZoneManager.dataNodes(causalityToken, zoneId)
                    .thenCompose(dataNodes -> {
                        if (dataNodes.isEmpty()) {
                            return completedFuture(null);
                        }

                        List<CatalogTableDescriptor> tableDescriptors = findTablesByZoneId(zoneId, parameters.catalogVersion());

                        List<CompletableFuture<?>> tableFutures = new ArrayList<>(tableDescriptors.size());

                        for (CatalogTableDescriptor tableDescriptor : tableDescriptors) {
                            LOG.info(
                                    "Received update for replicas number [table={}/{}, oldNumber={}, newNumber={}]",
                                    tableDescriptor.id(), tableDescriptor.name(), oldReplicas, parameters.zoneDescriptor().replicas()
                            );

                            CompletableFuture<?>[] partitionFutures = RebalanceUtil.triggerAllTablePartitionsRebalance(
                                    tableDescriptor,
                                    parameters.zoneDescriptor(),
                                    dataNodes,
                                    causalityToken,
                                    metaStorageManager
                            );

                            tableFutures.add(allOf(partitionFutures));
                        }

                        return allOf(tableFutures.toArray(CompletableFuture[]::new));
                    });
        } finally {
            busyLock.leaveBusy();
        }
    }

    private List<CatalogTableDescriptor> findTablesByZoneId(int zoneId, int catalogVersion) {
        return catalogManager.tables(catalogVersion).stream()
                .filter(table -> table.zoneId() == zoneId)
                .collect(toList());
    }
}
