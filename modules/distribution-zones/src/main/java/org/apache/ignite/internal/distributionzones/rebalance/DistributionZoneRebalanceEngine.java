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
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.extractZoneId;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.filterDataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.getZoneById;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.parseDataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneCatalogDescriptorUtils.toZoneDescriptor;
import static org.apache.ignite.internal.schema.CatalogDescriptorUtils.toTableDescriptor;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.Node;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneView;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.DistributionZoneNotFoundException;
import org.apache.ignite.lang.IgniteException;
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

    /** Distribution zone configuration. */
    private final DistributionZonesConfiguration zonesConfiguration;

    /** Tables configuration. */
    private final TablesConfiguration tablesConfiguration;

    /** Meta Storage manager. */
    private final MetaStorageManager metaStorageManager;

    /** Distribution zones manager. */
    private final DistributionZoneManager distributionZoneManager;

    /** Meta storage listener for data nodes changes. */
    private final WatchListener dataNodesListener;

    private final ConfigurationListener<Integer> onUpdateReplicas = this::onUpdateReplicas;

    /**
     * The constructor.
     *
     * @param stopGuard Prevents double stopping of the component.
     * @param busyLock Busy lock to stop synchronously.
     * @param zonesConfiguration Distribution zone configuration.
     * @param tablesConfiguration Tables configuration.
     * @param metaStorageManager Meta Storage manager.
     * @param distributionZoneManager Distribution zones manager.
     */
    public DistributionZoneRebalanceEngine(
            AtomicBoolean stopGuard,
            IgniteSpinBusyLock busyLock,
            DistributionZonesConfiguration zonesConfiguration,
            TablesConfiguration tablesConfiguration,
            MetaStorageManager metaStorageManager,
            DistributionZoneManager distributionZoneManager
    ) {
        this.stopGuard = stopGuard;
        this.busyLock = busyLock;
        this.zonesConfiguration = zonesConfiguration;
        this.tablesConfiguration = tablesConfiguration;
        this.metaStorageManager = metaStorageManager;
        this.distributionZoneManager = distributionZoneManager;

        this.dataNodesListener = createDistributionZonesDataNodesListener();
    }

    /**
     * Starts the rebalance engine by registering corresponding meta storage and configuration listeners.
     */
    public void start() {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            zonesConfiguration.defaultDistributionZone().replicas().listen(onUpdateReplicas);
            zonesConfiguration.distributionZones().any().replicas().listen(onUpdateReplicas);

            // TODO: IGNITE-18694 - Recovery for the case when zones watch listener processed event but assignments were not updated.
            metaStorageManager.registerPrefixWatch(zoneDataNodesKey(), dataNodesListener);
        } finally {
            busyLock.leaveBusy();
        }
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

        zonesConfiguration.defaultDistributionZone().replicas().stopListen(onUpdateReplicas);
        zonesConfiguration.distributionZones().any().replicas().stopListen(onUpdateReplicas);
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

                    DistributionZoneView zoneConfig;

                    try {
                        zoneConfig = getZoneById(zonesConfiguration, zoneId).value();
                    } catch (DistributionZoneNotFoundException e) {
                        //The zone was removed.
                        return completedFuture(null);
                    }

                    Set<String> filteredDataNodes = filterDataNodes(
                            dataNodes,
                            zoneConfig.filter(),
                            distributionZoneManager.nodesAttributes()
                    );

                    if (filteredDataNodes.isEmpty()) {
                        return completedFuture(null);
                    }

                    CatalogZoneDescriptor zoneDescriptor = toZoneDescriptor(zoneConfig);

                    for (TableView tableConfig : findTablesByZoneId(zoneId)) {
                        CatalogTableDescriptor tableDescriptor = toTableDescriptor(tableConfig);

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

    /**
     * Listener of replicas configuration changes.
     *
     * @param replicasCtx Replicas configuration event context.
     * @return A future, which will be completed, when event processed by listener.
     */
    private CompletableFuture<?> onUpdateReplicas(ConfigurationNotificationEvent<Integer> replicasCtx) {
        if (!busyLock.enterBusy()) {
            return completedFuture(new NodeStoppingException());
        }

        try {
            if (replicasCtx.oldValue() == null || replicasCtx.oldValue() <= 0) {
                return completedFuture(null);
            }

            DistributionZoneView zoneCfg = replicasCtx.newValue(DistributionZoneView.class);

            return distributionZoneManager.dataNodes(replicasCtx.storageRevision(), zoneCfg.zoneId())
                    .thenCompose(dataNodes -> {
                        if (dataNodes.isEmpty()) {
                            return completedFuture(null);
                        }

                        List<TableView> tableViews = findTablesByZoneId(zoneCfg.zoneId());

                        CatalogZoneDescriptor zoneDescriptor = toZoneDescriptor(zoneCfg);

                        List<CompletableFuture<?>> tableFutures = new ArrayList<>(tableViews.size());

                        for (TableView tableCfg : tableViews) {
                            CatalogTableDescriptor tableDescriptor = toTableDescriptor(tableCfg);

                            LOG.info("Received update for replicas number [table={}/{}, oldNumber={}, newNumber={}]",
                                    tableDescriptor.id(), tableDescriptor.name(), replicasCtx.oldValue(), replicasCtx.newValue());

                            CompletableFuture<?>[] partitionFutures = RebalanceUtil.triggerAllTablePartitionsRebalance(
                                    tableDescriptor,
                                    zoneDescriptor,
                                    dataNodes,
                                    replicasCtx.storageRevision(),
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

    private List<TableView> findTablesByZoneId(int zoneId) {
        return tablesConfiguration.tables().value().stream()
                .filter(table -> table.zoneId() == zoneId)
                .collect(toList());
    }
}
