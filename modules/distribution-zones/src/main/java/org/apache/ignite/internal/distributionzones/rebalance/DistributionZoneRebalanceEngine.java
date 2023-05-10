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
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.dataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.extractZoneId;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.getZoneById;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.updatePendingAssignmentsKeys;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneView;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.schema.configuration.ExtendedTableConfiguration;
import org.apache.ignite.internal.schema.configuration.TableChange;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
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
            zonesConfiguration.distributionZones().any().replicas().listen(this::onUpdateReplicas);

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
    }

    private WatchListener createDistributionZonesDataNodesListener() {
        return new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent evt) {
                if (!busyLock.enterBusy()) {
                    return failedFuture(new NodeStoppingException());
                }

                try {
                    byte[] dataNodesBytes = evt.entryEvent().newEntry().value();

                    if (dataNodesBytes == null) {
                        //The zone was removed so data nodes was removed too.
                        return completedFuture(null);
                    }

                    NamedConfigurationTree<TableConfiguration, TableView, TableChange> tables = tablesConfiguration.tables();

                    int zoneId = extractZoneId(evt.entryEvent().newEntry().key());

                    Set<String> dataNodes = dataNodes(ByteUtils.fromBytes(dataNodesBytes));

                    for (int i = 0; i < tables.value().size(); i++) {
                        TableView tableView = tables.value().get(i);

                        int tableZoneId = tableView.zoneId();

                        DistributionZoneConfiguration distributionZoneConfiguration =
                                getZoneById(zonesConfiguration, tableZoneId);

                        if (zoneId == tableZoneId) {
                            TableConfiguration tableCfg = tables.get(tableView.name());

                            byte[] assignmentsBytes = ((ExtendedTableConfiguration) tableCfg).assignments().value();

                            List<Set<Assignment>> tableAssignments = assignmentsBytes == null ?
                                    Collections.emptyList() :
                                    ByteUtils.fromBytes(assignmentsBytes);

                            for (int part = 0; part < distributionZoneConfiguration.partitions().value(); part++) {
                                UUID tableId = ((ExtendedTableConfiguration) tableCfg).id().value();

                                TablePartitionId replicaGrpId = new TablePartitionId(tableId, part);

                                int replicas = distributionZoneConfiguration.replicas().value();

                                int partId = part;

                                updatePendingAssignmentsKeys(
                                        tableView.name(),
                                        replicaGrpId,
                                        dataNodes,
                                        replicas,
                                        evt.entryEvent().newEntry().revision(),
                                        metaStorageManager,
                                        part,
                                        tableAssignments.isEmpty() ? Collections.emptySet() : tableAssignments.get(part)
                                ).exceptionally(e -> {
                                    LOG.error(
                                            "Exception on updating assignments for [table={}, partition={}]", e, tableView.name(),
                                            partId
                                    );

                                    return null;
                                });
                            }
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
            if (replicasCtx.oldValue() != null && replicasCtx.oldValue() > 0) {
                DistributionZoneView zoneCfg = replicasCtx.newValue(DistributionZoneView.class);

                List<TableConfiguration> tblsCfg = new ArrayList<>();

                tablesConfiguration.tables().value().namedListKeys().forEach(tblName -> {
                    if (tablesConfiguration.tables().get(tblName).zoneId().value().equals(zoneCfg.zoneId())) {
                        tblsCfg.add(tablesConfiguration.tables().get(tblName));
                    }
                });

                CompletableFuture<?>[] futs = new CompletableFuture[tblsCfg.size() * zoneCfg.partitions()];

                int furCur = 0;

                for (TableConfiguration tblCfg : tblsCfg) {

                    LOG.info("Received update for replicas number [table={}, oldNumber={}, newNumber={}]",
                            tblCfg.name().value(), replicasCtx.oldValue(), replicasCtx.newValue());

                    int partCnt = zoneCfg.partitions();

                    int newReplicas = replicasCtx.newValue();

                    byte[] assignmentsBytes = ((ExtendedTableConfiguration) tblCfg).assignments().value();

                    List<Set<Assignment>> tableAssignments = ByteUtils.fromBytes(assignmentsBytes);

                    for (int i = 0; i < partCnt; i++) {
                        TablePartitionId replicaGrpId = new TablePartitionId(((ExtendedTableConfiguration) tblCfg).id().value(), i);

                        futs[furCur++] = updatePendingAssignmentsKeys(
                                tblCfg.name().value(),
                                replicaGrpId,
                                distributionZoneManager.getDataNodesByZoneId(zoneCfg.zoneId()),
                                newReplicas,
                                replicasCtx.storageRevision(), metaStorageManager, i, tableAssignments.get(i));
                    }
                }
                return allOf(futs);
            } else {
                return completedFuture(null);
            }
        } finally {
            busyLock.leaveBusy();
        }
    }
}
