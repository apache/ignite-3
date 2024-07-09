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

package org.apache.ignite.internal.catalog;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_FILTER;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_PARTITION_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.DEFAULT_REPLICA_COUNT;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.clusterWideEnsuredActivationTsSafeForRoReads;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.defaultZoneIdOpt;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.LongSupplier;
import org.apache.ignite.internal.catalog.commands.AlterZoneSetDefaultCommand;
import org.apache.ignite.internal.catalog.commands.CreateSchemaCommand;
import org.apache.ignite.internal.catalog.commands.CreateZoneCommand;
import org.apache.ignite.internal.catalog.commands.StorageProfileParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.storage.Fireable;
import org.apache.ignite.internal.catalog.storage.SnapshotEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;
import org.apache.ignite.internal.catalog.storage.UpdateLog;
import org.apache.ignite.internal.catalog.storage.UpdateLog.OnUpdateHandler;
import org.apache.ignite.internal.catalog.storage.UpdateLogEvent;
import org.apache.ignite.internal.catalog.storage.VersionedUpdate;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViewProvider;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Catalog service implementation.
 */
public class CatalogManagerImpl extends AbstractEventProducer<CatalogEvent, CatalogEventParameters>
        implements CatalogManager, SystemViewProvider {
    /** Default zone name. */
    public static final String DEFAULT_ZONE_NAME = "Default";

    private static final int MAX_RETRY_COUNT = 10;

    /** Safe time to wait before new Catalog version activation. */
    static final int DEFAULT_DELAY_DURATION = 0;

    static final int DEFAULT_PARTITION_IDLE_SAFE_TIME_PROPAGATION_PERIOD = 0;

    /**
     * Initial update token for a catalog descriptor, this token is valid only before the first call of
     * {@link UpdateEntry#applyUpdate(Catalog, long)}.
     *
     * <p>After that {@link CatalogObjectDescriptor#updateToken()} will be initialised with a causality token from
     * {@link UpdateEntry#applyUpdate(Catalog, long)}
     */
    public static final long INITIAL_CAUSALITY_TOKEN = 0L;

    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(CatalogManagerImpl.class);

    /** Versioned catalog descriptors. */
    private final NavigableMap<Integer, Catalog> catalogByVer = new ConcurrentSkipListMap<>();

    /** Versioned catalog descriptors sorted in chronological order. */
    private final NavigableMap<Long, Catalog> catalogByTs = new ConcurrentSkipListMap<>();

    /** A future that completes when an empty catalog is initialised. If catalog is not empty this future when this completes starts. */
    private final CompletableFuture<Void> catalogInitializationFuture = new CompletableFuture<>();

    private final UpdateLog updateLog;

    private final PendingComparableValuesTracker<Integer, Void> versionTracker = new PendingComparableValuesTracker<>(0);

    private final ClockService clockService;

    private final LongSupplier delayDurationMsSupplier;

    private final LongSupplier partitionIdleSafeTimePropagationPeriodMsSupplier;

    private final CatalogSystemViewRegistry catalogSystemViewProvider;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /**
     * Node that is considered to be a coordinator of compaction process.
     *
     * <p>May be not set. Node should act as coordinator only in case this field is set and value is equal to name of the local node.
     */
    private volatile @Nullable String compactionCoordinatorNodeName;

    /**
     * Constructor.
     */
    public CatalogManagerImpl(UpdateLog updateLog, ClockService clockService) {
        this(updateLog, clockService, () -> DEFAULT_DELAY_DURATION, () -> DEFAULT_PARTITION_IDLE_SAFE_TIME_PROPAGATION_PERIOD);
    }

    /**
     * Constructor.
     */
    public CatalogManagerImpl(
            UpdateLog updateLog,
            ClockService clockService,
            LongSupplier delayDurationMsSupplier,
            LongSupplier partitionIdleSafeTimePropagationPeriodMsSupplier
    ) {
        this.updateLog = updateLog;
        this.clockService = clockService;
        this.delayDurationMsSupplier = delayDurationMsSupplier;
        this.partitionIdleSafeTimePropagationPeriodMsSupplier = partitionIdleSafeTimePropagationPeriodMsSupplier;
        this.catalogSystemViewProvider = new CatalogSystemViewRegistry(() -> catalogAt(clockService.nowLong()));
    }

    /** Updates the local view of the node with new compaction coordinator. */
    public void updateCompactionCoordinator(ClusterNode newCoordinator) {
        compactionCoordinatorNodeName = newCoordinator.name();
    }

    /** Returns local view of the node on who is currently compaction coordinator. For test purposes only.*/
    public @Nullable String compactionCoordinator() {
        return compactionCoordinatorNodeName;
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        int objectIdGen = 0;

        Catalog emptyCatalog = new Catalog(0, 0L, objectIdGen, List.of(), List.of(), null);

        registerCatalog(emptyCatalog);

        updateLog.registerUpdateHandler(new OnUpdateHandlerImpl());

        return updateLog.startAsync(componentContext)
                .thenComposeAsync(none -> {
                    if (latestCatalogVersion() == emptyCatalog.version()) {
                        int initializedCatalogVersion = emptyCatalog.version() + 1;

                        this.catalogReadyFuture(initializedCatalogVersion)
                                .thenComposeAsync(ignored -> awaitVersionActivation(initializedCatalogVersion),
                                        componentContext.executor())
                                .handleAsync((r, e) -> catalogInitializationFuture.complete(null), componentContext.executor());

                        return initCatalog(emptyCatalog);
                    } else {
                        catalogInitializationFuture.complete(null);
                        return nullCompletedFuture();
                    }
                }, componentContext.executor());
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        busyLock.block();
        versionTracker.close();
        return updateLog.stopAsync(componentContext);
    }

    @Override
    public @Nullable CatalogTableDescriptor table(String tableName, long timestamp) {
        CatalogSchemaDescriptor schema = catalogAt(timestamp).schema(SqlCommon.DEFAULT_SCHEMA_NAME);
        if (schema == null) {
            return null;
        }
        return schema.table(tableName);
    }

    @Override
    public @Nullable CatalogTableDescriptor table(int tableId, long timestamp) {
        return catalogAt(timestamp).table(tableId);
    }

    @Override
    public @Nullable CatalogTableDescriptor table(int tableId, int catalogVersion) {
        return catalog(catalogVersion).table(tableId);
    }

    @Override
    public Collection<CatalogTableDescriptor> tables(int catalogVersion) {
        return catalog(catalogVersion).tables();
    }

    @Override
    public @Nullable CatalogIndexDescriptor aliveIndex(String indexName, long timestamp) {
        CatalogSchemaDescriptor schema = catalogAt(timestamp).schema(SqlCommon.DEFAULT_SCHEMA_NAME);
        if (schema == null) {
            return null;
        }
        return schema.aliveIndex(indexName);
    }

    @Override
    public @Nullable CatalogIndexDescriptor index(int indexId, long timestamp) {
        return catalogAt(timestamp).index(indexId);
    }

    @Override
    public @Nullable CatalogIndexDescriptor index(int indexId, int catalogVersion) {
        return catalog(catalogVersion).index(indexId);
    }

    @Override
    public Collection<CatalogIndexDescriptor> indexes(int catalogVersion) {
        return catalog(catalogVersion).indexes();
    }

    @Override
    public List<CatalogIndexDescriptor> indexes(int catalogVersion, int tableId) {
        return catalog(catalogVersion).indexes(tableId);
    }

    @Override
    public @Nullable CatalogSchemaDescriptor schema(int catalogVersion) {
        return schema(SqlCommon.DEFAULT_SCHEMA_NAME, catalogVersion);
    }

    @Override
    public @Nullable CatalogSchemaDescriptor schema(String schemaName, int catalogVersion) {
        Catalog catalog = catalog(catalogVersion);

        if (catalog == null) {
            return null;
        }

        return catalog.schema(schemaName == null ? SqlCommon.DEFAULT_SCHEMA_NAME : schemaName);
    }

    @Override
    public @Nullable CatalogSchemaDescriptor schema(int schemaId, int catalogVersion) {
        Catalog catalog = catalog(catalogVersion);

        return catalog == null ? null : catalog.schema(schemaId);
    }

    @Override
    public @Nullable CatalogZoneDescriptor zone(String zoneName, long timestamp) {
        return catalogAt(timestamp).zone(zoneName);
    }

    @Override
    public @Nullable CatalogZoneDescriptor zone(int zoneId, long timestamp) {
        return catalogAt(timestamp).zone(zoneId);
    }

    @Override
    public @Nullable CatalogZoneDescriptor zone(int zoneId, int catalogVersion) {
        return catalog(catalogVersion).zone(zoneId);
    }

    @Override
    public Collection<CatalogZoneDescriptor> zones(int catalogVersion) {
        return catalog(catalogVersion).zones();
    }

    @Override
    public @Nullable CatalogSchemaDescriptor activeSchema(long timestamp) {
        return catalogAt(timestamp).schema(SqlCommon.DEFAULT_SCHEMA_NAME);
    }

    @Override
    public @Nullable CatalogSchemaDescriptor activeSchema(String schemaName, long timestamp) {
        return catalogAt(timestamp).schema(schemaName == null ? SqlCommon.DEFAULT_SCHEMA_NAME : schemaName);
    }

    @Override
    public int activeCatalogVersion(long timestamp) {
        return catalogAt(timestamp).version();
    }

    @Override
    public int earliestCatalogVersion() {
        return catalogByVer.firstEntry().getKey();
    }

    @Override
    public int latestCatalogVersion() {
        return catalogByVer.lastEntry().getKey();
    }

    @Override
    public CompletableFuture<Void> catalogReadyFuture(int version) {
        return versionTracker.waitFor(version);
    }

    @Override
    public CompletableFuture<Void> catalogInitializationFuture() {
        return catalogInitializationFuture;
    }

    @Override
    public @Nullable Catalog catalog(int catalogVersion) {
        return catalogByVer.get(catalogVersion);
    }

    private Catalog catalogAt(long timestamp) {
        Entry<Long, Catalog> entry = catalogByTs.floorEntry(timestamp);

        if (entry == null) {
            throw new IllegalStateException("No valid schema found for given timestamp: " + timestamp);
        }

        return entry.getValue();
    }

    @Override
    public CompletableFuture<Integer> execute(CatalogCommand command) {
        return saveUpdateAndWaitForActivation(command);
    }

    @Override
    public CompletableFuture<Integer> execute(List<CatalogCommand> commands) {
        if (nullOrEmpty(commands)) {
            return nullCompletedFuture();
        }

        return saveUpdateAndWaitForActivation(new BulkUpdateProducer(List.copyOf(commands)));
    }

    /**
     * Cleanup outdated catalog versions, which can't be observed after given timestamp (inclusively), and compact underlying update log.
     *
     * @param timestamp Earliest observable timestamp.
     * @return Operation future, which is completing with {@code true} if a new snapshot has been successfully written, {@code false}
     *         otherwise if a snapshot with the same or greater version already exists.
     */
    public CompletableFuture<Boolean> compactCatalog(long timestamp) {
        Catalog catalog = catalogAt(timestamp);

        return updateLog.saveSnapshot(new SnapshotEntry(catalog));
    }

    private CompletableFuture<Void> initCatalog(Catalog emptyCatalog) {
        List<CatalogCommand> initCommands = List.of(
                // Init default zone
                CreateZoneCommand.builder()
                        .zoneName(DEFAULT_ZONE_NAME)
                        .partitions(DEFAULT_PARTITION_COUNT)
                        .replicas(DEFAULT_REPLICA_COUNT)
                        .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                        .dataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE)
                        .filter(DEFAULT_FILTER)
                        .storageProfilesParams(
                                List.of(StorageProfileParams.builder().storageProfile(CatalogService.DEFAULT_STORAGE_PROFILE).build())
                        )
                        .build(),
                AlterZoneSetDefaultCommand.builder()
                        .zoneName(DEFAULT_ZONE_NAME)
                        .build(),
                // Add schemas
                CreateSchemaCommand.builder().name(SqlCommon.DEFAULT_SCHEMA_NAME).build(),
                CreateSchemaCommand.builder().name(SYSTEM_SCHEMA_NAME).build()
        );

        List<UpdateEntry> entries = new BulkUpdateProducer(initCommands).get(emptyCatalog);

        return updateLog.append(new VersionedUpdate(emptyCatalog.version() + 1, 0L, entries))
                .handle((result, error) -> {
                    if (error != null) {
                        LOG.warn("Unable to create default zone.", error);
                    }

                    return null;
                });
    }

    private void registerCatalog(Catalog newCatalog) {
        catalogByVer.put(newCatalog.version(), newCatalog);
        catalogByTs.put(newCatalog.time(), newCatalog);
    }

    private void truncateUpTo(Catalog catalog) {
        catalogByVer.headMap(catalog.version(), false).clear();
        catalogByTs.headMap(catalog.time(), false).clear();

        LOG.info("Catalog history was truncated up to version=" + catalog.version());
    }

    private CompletableFuture<Integer> saveUpdateAndWaitForActivation(UpdateProducer updateProducer) {
        CompletableFuture<Integer> resultFuture = new CompletableFuture<>();

        saveUpdate(updateProducer, 0)
                .thenCompose(this::awaitVersionActivation)
                .whenComplete((newVersion, err) -> {
                    if (err != null) {
                        Throwable errUnwrapped = ExceptionUtils.unwrapCause(err);

                        if (errUnwrapped instanceof CatalogVersionAwareValidationException) {
                            CatalogVersionAwareValidationException err0 = (CatalogVersionAwareValidationException) errUnwrapped;
                            Catalog catalog = catalogByVer.get(err0.version());
                            Throwable error = err0.initial();

                            if (catalog.version() == 0) {
                                resultFuture.completeExceptionally(error);
                            } else {
                                HybridTimestamp tsSafeForRoReadingInPastOptimization = calcClusterWideEnsureActivationTime(catalog);

                                clockService.waitFor(tsSafeForRoReadingInPastOptimization)
                                        .whenComplete((ver, err1) -> {
                                            if (err1 != null) {
                                                error.addSuppressed(err1);
                                            }

                                            resultFuture.completeExceptionally(error);
                                        });
                            }
                        } else {
                            resultFuture.completeExceptionally(err);
                        }
                    } else {
                        resultFuture.complete(newVersion);
                    }
                });

        return resultFuture;
    }

    private CompletableFuture<Integer> awaitVersionActivation(int version) {
        Catalog catalog = catalogByVer.get(version);

        HybridTimestamp tsSafeForRoReadingInPastOptimization = calcClusterWideEnsureActivationTime(catalog);

        return clockService.waitFor(tsSafeForRoReadingInPastOptimization).thenApply(unused -> version);
    }

    private HybridTimestamp calcClusterWideEnsureActivationTime(Catalog catalog) {
        return clusterWideEnsuredActivationTsSafeForRoReads(
                catalog,
                partitionIdleSafeTimePropagationPeriodMsSupplier,
                clockService.maxClockSkewMillis());
    }

    /**
     * Attempts to save a versioned update using a CAS-like logic. If the attempt fails, makes more attempts
     * until the max retry count is reached.
     *
     * @param updateProducer Supplies simple updates to include into a versioned update to install.
     * @param attemptNo Ordinal number of an attempt.
     * @return Future that completes with the new Catalog version (if update was saved successfully) or an exception, otherwise.
     */
    private CompletableFuture<Integer> saveUpdate(UpdateProducer updateProducer, int attemptNo) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            if (attemptNo >= MAX_RETRY_COUNT) {
                return failedFuture(new IgniteInternalException(Common.INTERNAL_ERR, "Max retry limit exceeded: " + attemptNo));
            }

            Catalog catalog = catalogByVer.lastEntry().getValue();

            List<UpdateEntry> updates;
            try {
                updates = updateProducer.get(catalog);
            } catch (CatalogValidationException ex) {
                return failedFuture(new CatalogVersionAwareValidationException(ex, catalog.version()));
            } catch (Exception ex) {
                return failedFuture(ex);
            }

            if (updates.isEmpty()) {
                return completedFuture(catalog.version());
            }

            int newVersion = catalog.version() + 1;

            // It is quite important to preserve such behavior: we wait here for versionTracker to be updated. It is updated when all events
            // that were triggered by this change will be completed. That means that any Catalog update will be completed only
            // after all reactions to that event will be completed through the catalog event notifications mechanism.
            // This is important for the distribution zones recovery purposes:
            // we guarantee recovery for a zones' catalog actions only if that actions were completed.
            return updateLog.append(new VersionedUpdate(newVersion, delayDurationMsSupplier.getAsLong(), updates))
                    .thenCompose(result -> versionTracker.waitFor(newVersion).thenApply(none -> result))
                    .thenCompose(result -> {
                        if (result) {
                            return completedFuture(newVersion);
                        }

                        return saveUpdate(updateProducer, attemptNo + 1);
                    });
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public List<SystemView<?>> systemViews() {
        return catalogSystemViewProvider.systemViews();
    }

    class OnUpdateHandlerImpl implements OnUpdateHandler {
        @Override
        public CompletableFuture<Void> handle(UpdateLogEvent event, HybridTimestamp metaStorageUpdateTimestamp, long causalityToken) {
            if (event instanceof SnapshotEntry) {
                return handle((SnapshotEntry) event);
            }

            return handle((VersionedUpdate) event, metaStorageUpdateTimestamp, causalityToken);
        }

        private CompletableFuture<Void> handle(SnapshotEntry event) {
            Catalog catalog = event.snapshot();
            // On recovery phase, we must register catalog from the snapshot.
            // In other cases, it is ok to rewrite an existed version, because it's exactly the same.
            registerCatalog(catalog);
            truncateUpTo(catalog);

            return nullCompletedFuture();
        }

        private CompletableFuture<Void> handle(VersionedUpdate update, HybridTimestamp metaStorageUpdateTimestamp, long causalityToken) {
            int version = update.version();
            Catalog catalog = catalogByVer.get(version - 1);

            assert catalog != null : version - 1;

            for (UpdateEntry entry : update.entries()) {
                catalog = entry.applyUpdate(catalog, causalityToken);
            }

            catalog = applyUpdateFinal(catalog, update, metaStorageUpdateTimestamp);

            registerCatalog(catalog);

            List<CompletableFuture<?>> eventFutures = new ArrayList<>(update.entries().size());

            for (UpdateEntry entry : update.entries()) {
                if (entry instanceof Fireable) {
                    Fireable fireEvent = (Fireable) entry;

                    eventFutures.add(fireEvent(
                            fireEvent.eventType(),
                            fireEvent.createEventParameters(causalityToken, version)
                    ));
                }
            }

            // It is quite important to preserve such behavior: we wait for all events to be completed and only after that we complete
            // versionTracker, which is used for saving any update to Catalog. That means that any Catalog update will be completed only
            // after all reactions to that event will be completed through the catalog event notifications mechanism.
            // This is important for the distribution zones recovery purposes:
            // we guarantee recovery for a zones' catalog actions only if that actions were completed.
            return allOf(eventFutures.toArray(CompletableFuture[]::new))
                    .whenComplete((ignore, err) -> {
                        if (err != null) {
                            LOG.warn("Failed to apply catalog update.", err);
                            // TODO: IGNITE-14611 Pass exception to an error handler because catalog got into inconsistent state.
                        }

                        versionTracker.update(version, null);
                    });
        }
    }

    private static Catalog applyUpdateFinal(Catalog catalog, VersionedUpdate update, HybridTimestamp metaStorageUpdateTimestamp) {
        long activationTimestamp = metaStorageUpdateTimestamp.addPhysicalTime(update.delayDurationMs()).longValue();

        assert activationTimestamp > catalog.time()
                : "Activation timestamp " + activationTimestamp + " must be greater than previous catalog version activation timestamp "
                        + catalog.time();

        return new Catalog(
                update.version(),
                activationTimestamp,
                catalog.objectIdGenState(),
                catalog.zones(),
                catalog.schemas(),
                defaultZoneIdOpt(catalog)
        );
    }

    private static class BulkUpdateProducer implements UpdateProducer {
        private final List<? extends UpdateProducer> commands;

        BulkUpdateProducer(List<? extends UpdateProducer> producers) {
            this.commands = producers;
        }

        @Override
        public List<UpdateEntry> get(Catalog catalog) {
            List<UpdateEntry> bulkUpdateEntries = new ArrayList<>();

            for (UpdateProducer producer : commands) {
                List<UpdateEntry> entries = producer.get(catalog);

                for (UpdateEntry entry : entries) {
                    catalog = entry.applyUpdate(catalog, INITIAL_CAUSALITY_TOKEN);
                }

                bulkUpdateEntries.addAll(entries);
            }

            return bulkUpdateEntries;
        }
    }
}
