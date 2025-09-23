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

package org.apache.ignite.internal.distributionzones;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptySet;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.CatalogManager.INITIAL_TIMESTAMP;
import static org.apache.ignite.internal.catalog.descriptors.ConsistencyMode.HIGH_AVAILABILITY;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.ZONE_ALTER;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.ZONE_CREATE;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.ZONE_DROP;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.PARTITION_DISTRIBUTION_RESET_TIMEOUT;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.PARTITION_DISTRIBUTION_RESET_TIMEOUT_DEFAULT_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.conditionForRecoverableStateChanges;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.deserializeLogicalTopologySet;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.filterDataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.updateLogicalTopologyAndVersion;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.updateLogicalTopologyAndVersionAndClusterId;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLastHandledTopology;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyClusterIdKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesNodesAttributes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesRecoverableStateRevision;
import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Statements.iif;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLongKeepingOrder;
import static org.apache.ignite.internal.util.ByteUtils.longToBytesKeepingOrder;
import static org.apache.ignite.internal.util.ByteUtils.uuidToBytes;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.AlterZoneEventParameters;
import org.apache.ignite.internal.catalog.events.CreateZoneEventParameters;
import org.apache.ignite.internal.catalog.events.DropZoneEventParameters;
import org.apache.ignite.internal.causality.RevisionListenerRegistry;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.components.NodeProperties;
import org.apache.ignite.internal.components.SystemPropertiesNodeProperties;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.utils.SystemDistributedConfigurationPropertyHolder;
import org.apache.ignite.internal.distributionzones.events.HaZoneTopologyUpdateEvent;
import org.apache.ignite.internal.distributionzones.events.HaZoneTopologyUpdateEventParams;
import org.apache.ignite.internal.distributionzones.exception.DistributionZoneNotFoundException;
import org.apache.ignite.internal.distributionzones.rebalance.DistributionZoneRebalanceEngine;
import org.apache.ignite.internal.distributionzones.utils.CatalogAlterZoneEventListener;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.failure.handlers.NoOpFailureHandler;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.Revisions;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.dsl.Update;
import org.apache.ignite.internal.metastorage.exceptions.CompactedException;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.TestOnly;

/**
 * Distribution zones manager.
 */
public class DistributionZoneManager extends
        AbstractEventProducer<HaZoneTopologyUpdateEvent, HaZoneTopologyUpdateEventParams> implements IgniteComponent {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(DistributionZoneManager.class);

    /** Meta Storage manager. */
    private final MetaStorageManager metaStorageManager;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Logical topology service to track topology changes. */
    private final LogicalTopologyService logicalTopologyService;

    private final FailureProcessor failureProcessor;

    private final DataNodesManager dataNodesManager;

    /** Listener for a topology events. */
    private final LogicalTopologyEventListener topologyEventListener = new DistributionZoneManagerLogicalTopologyEventListener();

    /**
     * The logical topology mapped to the MS revision.
     * TODO: https://issues.apache.org/jira/browse/IGNITE-24608 get rid of this map (or properly clean up it).
     */
    private final ConcurrentSkipListMap<Long, Set<NodeWithAttributes>> logicalTopologyByRevision = new ConcurrentSkipListMap<>();

    /**
     * Local mapping of {@code nodeId} -> node's attributes, where {@code nodeId} is a node id, that changes between restarts.
     * This map is updated every time we receive a topology event in a {@code topologyWatchListener}.
     * TODO: https://issues.apache.org/jira/browse/IGNITE-24608 properly clean up this map
     *
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/distribution-zones/tech-notes/filters.md">Filter documentation</a>
     */
    private Map<UUID, NodeWithAttributes> nodesAttributes = new ConcurrentHashMap<>();

    /** Watch listener for logical topology keys. */
    private final WatchListener topologyWatchListener;

    /** Rebalance engine. */
    private final DistributionZoneRebalanceEngine rebalanceEngine;

    /** Catalog manager. */
    private final CatalogManager catalogManager;

    /** Configuration of HA mode. */
    private final SystemDistributedConfigurationPropertyHolder<Integer> partitionDistributionResetTimeoutConfiguration;

    private final MetricManager metricManager;

    /** Mapping from a zone identifier to the corresponding metric source. */
    private final Map<Integer, ZoneMetricSource> zoneMetricSources = new ConcurrentHashMap<>();

    private final String localNodeName;

    /**
     * Constructor.
     */
    @TestOnly
    public DistributionZoneManager(
            String nodeName,
            RevisionListenerRegistry registry,
            MetaStorageManager metaStorageManager,
            LogicalTopologyService logicalTopologyService,
            CatalogManager catalogManager,
            SystemDistributedConfiguration systemDistributedConfiguration,
            ClockService clockService,
            MetricManager metricManager
    ) {
        this(
                nodeName,
                registry,
                metaStorageManager,
                logicalTopologyService,
                new FailureManager(new NoOpFailureHandler()),
                catalogManager,
                systemDistributedConfiguration,
                clockService,
                new SystemPropertiesNodeProperties(),
                metricManager
        );
    }

    /**
     * Creates a new distribution zone manager.
     *
     * @param nodeName Node name.
     * @param registry Registry for versioned values.
     * @param metaStorageManager Meta Storage manager.
     * @param logicalTopologyService Logical topology service.
     * @param failureProcessor Failure processor.
     * @param catalogManager Catalog manager.
     * @param systemDistributedConfiguration System distributed configuration.
     * @param clockService Clock service.
     */
    public DistributionZoneManager(
            String nodeName,
            RevisionListenerRegistry registry,
            MetaStorageManager metaStorageManager,
            LogicalTopologyService logicalTopologyService,
            FailureProcessor failureProcessor,
            CatalogManager catalogManager,
            SystemDistributedConfiguration systemDistributedConfiguration,
            ClockService clockService,
            NodeProperties nodeProperties,
            MetricManager metricManager
    ) {
        this.metaStorageManager = metaStorageManager;
        this.logicalTopologyService = logicalTopologyService;
        this.failureProcessor = failureProcessor;
        this.catalogManager = catalogManager;
        this.localNodeName = nodeName;

        this.topologyWatchListener = createMetastorageTopologyListener();

        // It's safe to leak with partially initialised object here, because rebalanceEngine is only accessible through this or by
        // meta storage notification thread that won't start before all components start.
        //noinspection ThisEscapedInObjectConstruction
        rebalanceEngine = new DistributionZoneRebalanceEngine(
                busyLock,
                metaStorageManager,
                this,
                catalogManager,
                nodeProperties
        );

        partitionDistributionResetTimeoutConfiguration = new SystemDistributedConfigurationPropertyHolder<>(
                systemDistributedConfiguration,
                this::onUpdatePartitionDistributionResetBusy,
                PARTITION_DISTRIBUTION_RESET_TIMEOUT,
                PARTITION_DISTRIBUTION_RESET_TIMEOUT_DEFAULT_VALUE,
                Integer::parseInt
        );

        dataNodesManager = new DataNodesManager(
                nodeName,
                busyLock,
                metaStorageManager,
                catalogManager,
                clockService,
                failureProcessor,
                this::fireTopologyReduceLocalEvent,
                partitionDistributionResetTimeoutConfiguration::currentValue
        );

        this.metricManager = metricManager;
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        return inBusyLockAsync(busyLock, () -> {
            partitionDistributionResetTimeoutConfiguration.init();

            registerCatalogEventListenersOnStartManagerBusy();

            logicalTopologyService.addEventListener(topologyEventListener);

            metaStorageManager.registerPrefixWatch(zonesLogicalTopologyPrefix(), topologyWatchListener);

            CompletableFuture<Revisions> recoveryFinishFuture = metaStorageManager.recoveryFinishedFuture();

            // At the moment of the start of this manager, it is guaranteed that Meta Storage has been recovered.
            assert recoveryFinishFuture.isDone();

            long recoveryRevision = recoveryFinishFuture.join().revision();

            restoreGlobalStateFromLocalMetaStorage(recoveryRevision);

            // If Catalog manager is empty, it gets initialized asynchronously and at this moment the initialization might not complete,
            // nevertheless everything works correctly.
            // All components execute the synchronous part of startAsync sequentially and only when they all complete,
            // we enable metastorage listeners (see IgniteImpl.joinClusterAsync: metaStorageMgr.deployWatches()).
            // Once the metstorage watches are deployed, all components start to receive callbacks, this chain of callbacks eventually
            // fires CatalogManager's ZONE_CREATE event, and the state of DistributionZoneManager becomes consistent.
            int catalogVersion = catalogManager.latestCatalogVersion();

            registerMetricSourcesOnStart();

            return allOf(
                    restoreLogicalTopologyChangeEvent(recoveryRevision),
                    dataNodesManager.startAsync(currentZones(), recoveryRevision)
            ).thenComposeAsync((notUsed) -> rebalanceEngine.startAsync(catalogVersion), componentContext.executor());
        });
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        if (!stopGuard.compareAndSet(false, true)) {
            return nullCompletedFuture();
        }

        busyLock.block();

        dataNodesManager.stop();

        rebalanceEngine.stop();

        logicalTopologyService.removeEventListener(topologyEventListener);

        metaStorageManager.unregisterWatch(topologyWatchListener);

        return nullCompletedFuture();
    }

    /**
     * Gets data nodes of the zone using causality token and catalog version. {@code timestamp} must be agreed
     * with the {@code catalogVersion}, meaning that for the provided {@code timestamp} actual {@code catalogVersion} must be provided.
     * For example, if you are in the meta storage watch thread and {@code timestamp} is the timestamp of the watch event, it is
     * safe to take {@link CatalogManager#latestCatalogVersion()} as a {@code catalogVersion},
     * because {@link CatalogManager#latestCatalogVersion()} won't be updated in a watch thread.
     *
     * <p>Return data nodes or throw the exception:
     * {@link IllegalArgumentException} if zoneId is not valid.
     * {@link DistributionZoneNotFoundException} if the zone with the provided zoneId does not exist.
     *
     * @param timestamp Timestamp.
     * @param catalogVersion Catalog version.
     * @param zoneId Zone id.
     * @return The future with data nodes for the zoneId.
     */
    public CompletableFuture<Set<String>> dataNodes(HybridTimestamp timestamp, int catalogVersion, int zoneId) {
        if (catalogVersion < 0) {
            throw new IllegalArgumentException("catalogVersion must be greater or equal to zero [catalogVersion=" + catalogVersion + '"');
        }

        if (zoneId < 0) {
            throw new IllegalArgumentException("zoneId cannot be a negative number [zoneId=" + zoneId + '"');
        }

        if (timestamp.equals(INITIAL_TIMESTAMP)) {
            timestamp = hybridTimestamp(catalogManager.catalog(catalogVersion).time());
        }

        return dataNodesManager.dataNodes(zoneId, timestamp, catalogVersion);
    }

    private CompletableFuture<Void> onUpdateScaleUpBusy(AlterZoneEventParameters parameters) {
        HybridTimestamp timestamp = metaStorageManager.timestampByRevisionLocally(parameters.causalityToken());

        return dataNodesManager.onAutoAdjustAlteration(parameters.zoneDescriptor(), timestamp);
    }

    private void onUpdatePartitionDistributionResetBusy(
            int partitionDistributionResetTimeoutSeconds,
            long causalityToken
    ) {
        CompletableFuture<Revisions> recoveryFuture = metaStorageManager.recoveryFinishedFuture();

        // At the moment of the first call to this method from configuration notifications,
        // it is guaranteed that Meta Storage has been recovered.
        assert recoveryFuture.isDone();

        if (recoveryFuture.join().revision() >= causalityToken) {
            // So, configuration already has the right value on configuration init
            // and all timers started with the right configuration timeouts on recovery.
            return;
        }

        // It is safe to zoneState.entrySet in term of ConcurrentModification and etc. because meta storage notifications are one-threaded
        // and this map will be initialized on a manager start or with catalog notification or with distribution configuration changes.
        for (CatalogZoneDescriptor zoneDescriptor : currentZones()) {
            int zoneId = zoneDescriptor.id();

            if (zoneDescriptor.consistencyMode() != HIGH_AVAILABILITY) {
                continue;
            }

            dataNodesManager.onUpdatePartitionDistributionReset(
                    zoneId,
                    partitionDistributionResetTimeoutSeconds,
                    () -> fireTopologyReduceLocalEvent(causalityToken, zoneId)
            );
        }
    }

    private CompletableFuture<Void> onUpdateScaleDownBusy(AlterZoneEventParameters parameters) {
        HybridTimestamp timestamp = metaStorageManager.timestampByRevisionLocally(parameters.causalityToken());

        return dataNodesManager.onAutoAdjustAlteration(parameters.zoneDescriptor(), timestamp);
    }

    private CompletableFuture<Void> onUpdateFilterBusy(AlterZoneEventParameters parameters) {
        HybridTimestamp timestamp = metaStorageManager.timestampByRevisionLocally(parameters.causalityToken());

        Entry topologyEntry = metaStorageManager.getLocally(zonesLogicalTopologyKey(), parameters.causalityToken());

        if (topologyEntry != null && topologyEntry.value() != null) {
            Set<NodeWithAttributes> logicalTopology = deserializeLogicalTopologySet(topologyEntry.value());

            return dataNodesManager.onZoneFilterChange(parameters.zoneDescriptor(), timestamp, logicalTopology);
        } else {
            return nullCompletedFuture();
        }
    }

    private CompletableFuture<Void> onCreateZone(CatalogZoneDescriptor zone, long causalityToken) {
        HybridTimestamp timestamp = metaStorageManager.timestampByRevisionLocally(causalityToken);

        return dataNodesManager
                .onZoneCreate(zone.id(), timestamp, filterDataNodes(logicalTopology(causalityToken), zone))
                .thenRun(() -> {
                    try {
                        registerMetricSource(zone);
                    } catch (Exception e) {
                        // This is not a critical error, so there is no need to stop node if we failed to register a metric source.
                        // So, just log the error.
                        LOG.error("Failed to register a new zone metric source [zoneDescriptor={}]", e, zone);
                    }
                });
    }

    /**
     * Updates {@link DistributionZonesUtil#zonesLogicalTopologyKey()} and {@link DistributionZonesUtil#zonesLogicalTopologyVersionKey()}
     * in meta storage.
     *
     * @param newTopology Logical topology snapshot.
     */
    private void updateLogicalTopologyInMetaStorage(LogicalTopologySnapshot newTopology) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            Set<LogicalNode> logicalTopology = newTopology.nodes();

            Condition condition;
            Update update;

            if (newTopology.version() == LogicalTopologySnapshot.FIRST_VERSION) {
                // Very first start of the cluster, OR first topology version after a cluster reset, so we just
                // initialize zonesLogicalTopologyVersionKey.
                // We don't need to check whether clusterId is 'newer' as it's guaranteed that after a newer clusterId
                // gets written to the Metastorage, we cannot send a Metastorage update switching it back to older clusterId.
                condition = notExists(zonesLogicalTopologyVersionKey())
                        .or(value(zonesLogicalTopologyClusterIdKey()).ne(uuidToBytes(newTopology.clusterId())));
                update = updateLogicalTopologyAndVersionAndClusterId(newTopology);
            } else {
                condition = value(zonesLogicalTopologyVersionKey()).lt(longToBytesKeepingOrder(newTopology.version()));
                update = updateLogicalTopologyAndVersion(newTopology);
            }

            Iif iff = iif(condition, update, ops().yield(false));

            metaStorageManager.invoke(iff).whenComplete((res, e) -> {
                if (e != null) {
                    if (!relatesToNodeStopping(e)) {
                        String errorMessage = String.format(
                                "Failed to update distribution zones' logical topology and version keys [topology = %s, version = %s]",
                                Arrays.toString(logicalTopology.toArray()),
                                newTopology.version()
                        );
                        failureProcessor.process(new FailureContext(e, errorMessage));
                    }
                } else if (res.getAsBoolean()) {
                    LOG.info(
                            "Distribution zones' logical topology and version keys were updated [topology = {}, version = {}]",
                            Arrays.toString(logicalTopology.toArray()),
                            newTopology.version()
                    );
                } else {
                    LOG.debug(
                            "Failed to update distribution zones' logical topology and version keys due to concurrent update ["
                                    + "topology = {}, version = {}]",
                            Arrays.toString(logicalTopology.toArray()),
                            newTopology.version()
                    );
                }
            });
        } finally {
            busyLock.leaveBusy();
        }
    }

    private static boolean relatesToNodeStopping(Throwable e) {
        return hasCause(e, NodeStoppingException.class);
    }

    /**
     * Restores from local Meta Storage logical topology and nodes' attributes fields in {@link DistributionZoneManager} after restart.
     *
     * @param recoveryRevision Revision of the Meta Storage after its recovery.
     */
    private void restoreGlobalStateFromLocalMetaStorage(long recoveryRevision) {
        Entry lastHandledTopologyEntry = metaStorageManager.getLocally(zonesLastHandledTopology(), recoveryRevision);

        Entry nodeAttributesEntry = metaStorageManager.getLocally(zonesNodesAttributes(), recoveryRevision);

        if (lastHandledTopologyEntry.value() != null) {
            // We save zonesLastHandledTopology and zonesNodesAttributes in Meta Storage in a one batch, so it is impossible
            // that one value is not null, but other is null.
            assert nodeAttributesEntry.value() != null;

            logicalTopologyByRevision.put(recoveryRevision, deserializeLogicalTopologySet(lastHandledTopologyEntry.value()));

            nodesAttributes = DistributionZonesUtil.deserializeNodesAttributes(nodeAttributesEntry.value());
        }

        assert lastHandledTopologyEntry.value() == null
                || logicalTopology(recoveryRevision).equals(deserializeLogicalTopologySet(lastHandledTopologyEntry.value()))
                : "Initial value of logical topology was changed after initialization from the Meta Storage manager.";

        assert nodeAttributesEntry.value() == null
                || nodesAttributes.equals(DistributionZonesUtil.deserializeNodesAttributes(nodeAttributesEntry.value()))
                : "Initial value of nodes' attributes was changed after initialization from the Meta Storage manager.";
    }

    /**
     * Creates watch listener which listens logical topology and logical topology version.
     *
     * @return Watch listener.
     */
    private WatchListener createMetastorageTopologyListener() {
        return evt -> {
            if (!busyLock.enterBusy()) {
                return failedFuture(new NodeStoppingException());
            }

            try {
                assert evt.entryEvents().size() == 2 || evt.entryEvents().size() == 3 :
                        "Expected an event with logical topology, its version and maybe clusterId entries but was events with keys: "
                        + evt.entryEvents().stream().map(DistributionZoneManager::entryKeyAsString)
                        .collect(toList());

                byte[] newLogicalTopologyBytes;

                Set<NodeWithAttributes> newLogicalTopology = null;
                Set<NodeWithAttributes> oldLogicalTopology = null;

                HybridTimestamp timestamp = evt.timestamp();

                for (EntryEvent event : evt.entryEvents()) {
                    Entry e = event.newEntry();
                    Entry old = event.oldEntry();

                    if (Arrays.equals(e.key(), zonesLogicalTopologyKey().bytes())) {
                        newLogicalTopologyBytes = e.value();

                        assert newLogicalTopologyBytes != null : "New topology is null.";

                        newLogicalTopology = deserializeLogicalTopologySet(newLogicalTopologyBytes);

                        byte[] oldLogicalTopologyBytes = old.value();

                        if (oldLogicalTopologyBytes != null) {
                            oldLogicalTopology = deserializeLogicalTopologySet(oldLogicalTopologyBytes);
                        }
                    }
                }

                assert newLogicalTopology != null : "The event doesn't contain logical topology";

                if (oldLogicalTopology == null) {
                    oldLogicalTopology = newLogicalTopology;
                }

                return onLogicalTopologyUpdate(newLogicalTopology, oldLogicalTopology, evt.revision(), timestamp);
            } finally {
                busyLock.leaveBusy();
            }
        };
    }

    private static String entryKeyAsString(EntryEvent entry) {
        return entry.newEntry() == null ? "null" : new String(entry.newEntry().key(), UTF_8);
    }

    /**
     * Reaction on an update of logical topology. In this method {@link DistributionZoneManager#logicalTopology},
     * {@link DistributionZoneManager#nodesAttributes} are updated.
     * This fields are saved to Meta Storage, also timers are scheduled.
     * Note that all futures of Meta Storage updates that happen in this method are returned from this method.
     *
     * @param newLogicalTopology New logical topology.
     * @param oldLogicalTopology Old logical topology.
     * @param revision Revision of the event.
     * @param timestamp Event timestamp.
     * @return Future reflecting the completion of the actions needed when logical topology was updated.
     */
    private CompletableFuture<Void> onLogicalTopologyUpdate(
            Set<NodeWithAttributes> newLogicalTopology,
            Set<NodeWithAttributes> oldLogicalTopology,
            long revision,
            HybridTimestamp timestamp
    ) {
        logicalTopologyByRevision.put(revision, newLogicalTopology);

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (CatalogZoneDescriptor zone : currentZones()) {
            CompletableFuture<Void> f = dataNodesManager.onTopologyChange(
                    zone,
                    revision,
                    timestamp,
                    newLogicalTopology,
                    oldLogicalTopology
            );

            futures.add(f);
        }

        newLogicalTopology.forEach(n -> nodesAttributes.put(n.nodeId(), n));

        futures.add(saveRecoverableStateToMetastorage(revision, newLogicalTopology));

        return allOf(futures.toArray(CompletableFuture[]::new));
    }

    /**
     * Returns the current zones in the Catalog. Must always be called from the meta storage thread.
     */
    private Collection<CatalogZoneDescriptor> currentZones() {
        int catalogVersion = catalogManager.latestCatalogVersion();

        return catalogManager.catalog(catalogVersion).zones();
    }

    /**
     * Saves recoverable state of the Distribution Zone Manager to Meta Storage atomically in one batch.
     * After restart it could be used to restore these fields.
     *
     * @param revision Revision of the event.
     * @param newLogicalTopology New logical topology.
     * @return Future representing pending completion of the operation.
     */
    private CompletableFuture<Void> saveRecoverableStateToMetastorage(
            long revision,
            Set<NodeWithAttributes> newLogicalTopology
    ) {
        Operation[] puts = {
                put(zonesNodesAttributes(), NodesAttributesSerializer.serialize(nodesAttributes())),
                put(zonesRecoverableStateRevision(), longToBytesKeepingOrder(revision)),
                put(
                        zonesLastHandledTopology(),
                        LogicalTopologySetSerializer.serialize(newLogicalTopology)
                )
        };

        Iif iif = iif(
                conditionForRecoverableStateChanges(revision),
                ops(puts).yield(true),
                ops().yield(false)
        );

        return metaStorageManager.invoke(iif)
                .thenApply(StatementResult::getAsBoolean)
                .whenComplete((invokeResult, e) -> {
                    if (e != null) {
                        if (!relatesToNodeStopping(e)) {
                            String errorMessage = String.format(
                                    "Failed to update recoverable state for distribution zone manager [revision = %s]",
                                    revision
                            );
                            failureProcessor.process(new FailureContext(e, errorMessage));
                        }
                    } else if (invokeResult) {
                        LOG.info("Update recoverable state for distribution zone manager [revision = {}]", revision);
                    } else {
                        LOG.debug("Failed to update recoverable states for distribution zone manager [revision = {}]", revision);
                    }
                }).thenCompose((ignored) -> nullCompletedFuture());
    }

    /**
     * Returns metastore long view of {@link HybridTimestamp} by revision.
     *
     * @param revision Metastore revision.
     * @return Appropriate metastore timestamp or -1 if revision is already compacted.
     */
    private long timestampByRevision(long revision) {
        try {
            return metaStorageManager.timestampByRevisionLocally(revision).longValue();
        } catch (CompactedException e) {
            if (revision > 1) {
                LOG.warn("Unable to retrieve timestamp by revision because of meta storage compaction, [revision={}].", revision);
            }

            return -1;
        }
    }

    private void fireTopologyReduceLocalEvent(long revision, int zoneId) {
        fireEvent(
                HaZoneTopologyUpdateEvent.TOPOLOGY_REDUCED,
                new HaZoneTopologyUpdateEventParams(zoneId, revision)
        ).exceptionally(th -> {
            LOG.error("Error during the local " + HaZoneTopologyUpdateEvent.TOPOLOGY_REDUCED.name()
                    + " event processing", th);

            return null;
        });
    }

    @TestOnly
    public DataNodesManager dataNodesManager() {
        return dataNodesManager;
    }

    /**
     * Returns local mapping of {@code nodeId} -> node's attributes, where {@code nodeId} is a node id, that changes between restarts.
     * This map is updated every time we receive a topology event in a {@code topologyWatchListener}.
     *
     * @return Mapping {@code nodeId} -> node's attributes.
     */
    public Map<UUID, NodeWithAttributes> nodesAttributes() {
        return nodesAttributes;
    }

    public Set<NodeWithAttributes> logicalTopology() {
        return logicalTopology(Long.MAX_VALUE);
    }

    /**
     * Get logical topology for the given revision.
     * If there is no data for revision i, return topology for the maximum revision smaller than i.
     *
     * @param revision metastore revision.
     * @return logical topology.
     */
    public Set<NodeWithAttributes> logicalTopology(long revision) {
        assert revision >= 0 : revision;

        Map.Entry<Long, Set<NodeWithAttributes>> entry = logicalTopologyByRevision.floorEntry(revision);

        return entry != null ? entry.getValue() : emptySet();
    }

    private void registerCatalogEventListenersOnStartManagerBusy() {
        catalogManager.listen(ZONE_CREATE, (CreateZoneEventParameters parameters) -> inBusyLock(busyLock, () -> {
            return onCreateZone(parameters.zoneDescriptor(), parameters.causalityToken()).thenApply((ignored) -> false);
        }));

        catalogManager.listen(ZONE_DROP, (DropZoneEventParameters parameters) -> inBusyLock(busyLock, () -> {
            return onDropZoneBusy(parameters).thenApply((ignored) -> false);
        }));

        catalogManager.listen(ZONE_ALTER, new ManagerCatalogAlterZoneEventListener());
    }

    /**
     * Registers metric source for the specified zone.
     *
     * @param zone Zone descriptor.
     */
    private void registerMetricSource(CatalogZoneDescriptor zone) {
        ZoneMetricSource source = new ZoneMetricSource(metaStorageManager, localNodeName, zone);

        zoneMetricSources.put(zone.id(), source);

        metricManager.registerSource(source);
        metricManager.enable(source);
    }

    /**
     * Registers zone metric sources on node starting.
     */
    private void registerMetricSourcesOnStart() {
        currentZones().forEach(this::registerMetricSource);
    }

    /**
     * Restore the event of the updating the logical topology from Meta Storage, that has not been completed before restart.
     *
     * @param recoveryRevision Revision of the Meta Storage after its recovery.
     * @return Future that represents the pending completion of the operations.
     */
    private CompletableFuture<Void> restoreLogicalTopologyChangeEvent(long recoveryRevision) {
        Entry topologyEntry = metaStorageManager.getLocally(zonesLogicalTopologyKey(), recoveryRevision);

        if (topologyEntry.value() != null) {
            Set<NodeWithAttributes> logicalTopology = deserializeLogicalTopologySet(topologyEntry.value());

            long topologyRevision = topologyEntry.revision();

            Entry lastUpdateRevisionEntry = metaStorageManager.getLocally(zonesRecoverableStateRevision(), recoveryRevision);

            if (lastUpdateRevisionEntry.value() == null || topologyRevision > bytesToLongKeepingOrder(lastUpdateRevisionEntry.value())) {
                HybridTimestamp timestamp = metaStorageManager.timestampByRevisionLocally(recoveryRevision);

                return onLogicalTopologyUpdate(logicalTopology, logicalTopology, recoveryRevision, timestamp);
            }
        }

        return nullCompletedFuture();
    }

    private CompletableFuture<?> onDropZoneBusy(DropZoneEventParameters parameters) {
        try {
            ZoneMetricSource source = zoneMetricSources.remove(parameters.zoneId());
            if (source != null) {
                metricManager.unregisterSource(source);
            }
        } catch (Exception e) {
            LOG.error("Failed to unregister zone metric source [dropZoneEvent={}]", e, parameters);
        }

        long causalityToken = parameters.causalityToken();

        HybridTimestamp timestamp = metaStorageManager.timestampByRevisionLocally(causalityToken);

        return dataNodesManager.onZoneDrop(parameters.zoneId(), timestamp);
    }

    private class ManagerCatalogAlterZoneEventListener extends CatalogAlterZoneEventListener {
        private ManagerCatalogAlterZoneEventListener() {
            super(catalogManager);
        }

        @Override
        protected CompletableFuture<Void> onAutoAdjustScaleUpUpdate(AlterZoneEventParameters parameters, int oldAutoAdjustScaleUp) {
            return inBusyLock(busyLock, () -> onUpdateScaleUpBusy(parameters));
        }

        @Override
        protected CompletableFuture<Void> onAutoAdjustScaleDownUpdate(AlterZoneEventParameters parameters, int oldAutoAdjustScaleDown) {
            return inBusyLock(busyLock, () -> onUpdateScaleDownBusy(parameters));
        }

        @Override
        protected CompletableFuture<Void> onFilterUpdate(AlterZoneEventParameters parameters, String oldFilter) {
            return inBusyLock(busyLock, () -> onUpdateFilterBusy(parameters));
        }

        @Override
        protected CompletableFuture<Void> onNameUpdate(AlterZoneEventParameters parameters, String oldName) {
            return inBusyLock(busyLock, () -> {
                try {
                    CatalogZoneDescriptor zoneDescriptor = parameters.zoneDescriptor();

                    // Update metric source name.
                    ZoneMetricSource source = zoneMetricSources.remove(zoneDescriptor.id());
                    if (source != null) {
                        metricManager.unregisterSource(source);
                    }

                    registerMetricSource(parameters.zoneDescriptor());
                } catch (Exception e) {
                    // This is not a critical error, so there is no need to stop node if we failed to register a metric source.
                    // So, just log the error.
                    LOG.error("Failed to update zone metric set [alterZoneEvent={}]", e, parameters);
                }

                return nullCompletedFuture();
            });
        }
    }

    private class DistributionZoneManagerLogicalTopologyEventListener implements LogicalTopologyEventListener {
        @Override
        public void onNodeJoined(LogicalNode joinedNode, LogicalTopologySnapshot newTopology) {
            updateLogicalTopologyInMetaStorage(newTopology);
        }

        @Override
        public void onNodeLeft(LogicalNode leftNode, LogicalTopologySnapshot newTopology) {
            updateLogicalTopologyInMetaStorage(newTopology);
        }

        @Override
        public void onTopologyLeap(LogicalTopologySnapshot newTopology) {
            updateLogicalTopologyInMetaStorage(newTopology);
        }
    }
}
