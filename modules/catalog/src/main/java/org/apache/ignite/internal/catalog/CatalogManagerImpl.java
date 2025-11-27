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
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.clusterWideEnsuredActivationTimestamp;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.defaultZoneIdOpt;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;
import org.apache.ignite.internal.catalog.commands.CreateSchemaCommand;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
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
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
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

/**
 * Catalog service implementation.
 */
public class CatalogManagerImpl extends AbstractEventProducer<CatalogEvent, CatalogEventParameters>
        implements CatalogManager, SystemViewProvider {
    private static final int MAX_RETRY_COUNT = 10;

    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(CatalogManagerImpl.class);

    /** Versioned catalog descriptors. */
    private volatile CatalogByIndexMap catalogByVer = new CatalogByIndexMap();

    /** Versioned catalog descriptors sorted in chronological order. */
    private volatile CatalogByIndexMap catalogByTs = new CatalogByIndexMap();

    /** A future that completes when an empty catalog is initialised. If catalog is not empty this future when this completes starts. */
    private final CompletableFuture<Void> catalogInitializationFuture = new CompletableFuture<>();

    private final UpdateLog updateLog;

    private final PendingComparableValuesTracker<Integer, Void> versionTracker = new PendingComparableValuesTracker<>(0);

    private final ClockService clockService;

    private final FailureProcessor failureProcessor;

    private final LongSupplier delayDurationMsSupplier;

    private final CatalogSystemViewRegistry catalogSystemViewProvider;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /**
     * Future used to chain local appends to UpdateLog to avoid useless concurrency (as all concurrent attempts to append compete for the
     * same catalog version, hence only one will win and the rest will have to retry).
     *
     * <p>Guarded by {@link #lastSaveUpdateFutureMutex}.
     */
    private CompletableFuture<Void> lastSaveUpdateFuture = nullCompletedFuture();

    /**
     * Guards access to {@link #lastSaveUpdateFuture}.
     */
    private final Object lastSaveUpdateFutureMutex = new Object();

    /**
     * Constructor.
     */
    public CatalogManagerImpl(
            UpdateLog updateLog,
            ClockService clockService,
            FailureProcessor failureProcessor,
            LongSupplier delayDurationMsSupplier
    ) {
        this.updateLog = updateLog;
        this.clockService = clockService;
        this.failureProcessor = failureProcessor;
        this.delayDurationMsSupplier = delayDurationMsSupplier;
        this.catalogSystemViewProvider = new CatalogSystemViewRegistry(() -> catalogAt(clockService.nowLong()));
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        int objectIdGen = 0;

        Catalog emptyCatalog = new Catalog(0, HybridTimestamp.MIN_VALUE.longValue(), objectIdGen, List.of(), List.of(), null);

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
        versionTracker.close(new NodeStoppingException());
        return updateLog.stopAsync(componentContext);
    }

    @Override
    public int activeCatalogVersion(long timestamp) {
        return catalogAt(timestamp).version();
    }

    @Override
    public int earliestCatalogVersion() {
        return (int) catalogByVer.firstKey();
    }

    @Override
    public Catalog earliestCatalog() {
        return catalogByVer.firstValue();
    }

    @Override
    public Catalog latestCatalog() {
        return catalogByVer.lastValue();
    }

    @Override
    public CompletableFuture<Void> catalogReadyFuture(int version) {
        return inBusyLockAsync(busyLock, () -> versionTracker.waitFor(version));
    }

    @Override
    public CompletableFuture<Void> catalogInitializationFuture() {
        return catalogInitializationFuture;
    }

    @Override
    public Catalog catalog(int catalogVersion) {
        Catalog catalog = catalogByVer.get(catalogVersion);

        if (catalog == null) {
            throw new CatalogNotFoundException("Catalog version not found: " + catalogVersion);
        }

        return catalog;
    }

    @Override
    public Catalog activeCatalog(long timestamp) {
        return catalogAt(timestamp);
    }

    private Catalog catalogAt(long timestamp) {
        Catalog catalog = catalogByTs.floorValue(timestamp);

        if (catalog == null) {
            throw new CatalogNotFoundException("Catalog not found for given timestamp: " + timestamp);
        }

        return catalog;
    }

    @Override
    public CompletableFuture<CatalogApplyResult> execute(CatalogCommand command) {
        return saveUpdateAndWaitForActivation(List.of(command));
    }

    @Override
    public CompletableFuture<CatalogApplyResult> execute(List<CatalogCommand> commands) {
        if (nullOrEmpty(commands)) {
            return nullCompletedFuture();
        }

        return saveUpdateAndWaitForActivation(List.copyOf(commands));
    }

    /**
     * Cleanup outdated catalog versions, which can't be observed after given timestamp (inclusively), and compact underlying update log.
     *
     * @param version Earliest observable version.
     * @return Operation future, which is completing with {@code true} if a new snapshot has been successfully written, {@code false}
     *         otherwise if a snapshot with the same or greater version already exists.
     */
    public CompletableFuture<Boolean> compactCatalog(int version) {
        Catalog catalog = catalog(version);

        if (catalog == null) {
            throw new IllegalArgumentException("Catalog version not found: " + version);
        }

        return updateLog.saveSnapshot(new SnapshotEntry(catalog));
    }

    private CompletableFuture<Void> initCatalog(Catalog emptyCatalog) {
        List<CatalogCommand> initCommands = List.of(
                // Add schemas
                CreateSchemaCommand.builder().name(SqlCommon.DEFAULT_SCHEMA_NAME).build(),
                CreateSchemaCommand.systemSchemaBuilder().name(SYSTEM_SCHEMA_NAME).build()
        );

        List<UpdateEntry> entries = new BulkUpdateProducer(initCommands).get(new UpdateContext(emptyCatalog));

        return updateLog.append(new VersionedUpdate(emptyCatalog.version() + 1, 0L, entries))
                .handle((result, error) -> {
                    if (error != null && !hasCause(error, NodeStoppingException.class)) {
                        failureProcessor.process(new FailureContext(error, "Unable to create default zone."));
                    }

                    return null;
                });
    }

    private void registerCatalog(Catalog newCatalog) {
        catalogByVer = catalogByVer.appendOrUpdate(newCatalog.version(), newCatalog);
        catalogByTs = catalogByTs.appendOrUpdate(newCatalog.time(), newCatalog);
    }

    private void truncateUpTo(Catalog catalog) {
        catalogByVer = catalogByVer.clearHead(catalog.version());
        catalogByTs = catalogByTs.clearHead(catalog.time());

        LOG.info("Catalog history was truncated up to version=" + catalog.version());
    }

    private CompletableFuture<CatalogApplyResult> saveUpdateAndWaitForActivation(List<UpdateProducer> updateProducers) {
        CompletableFuture<CatalogApplyResult> resultFuture = new CompletableFuture<>();

        saveUpdateEliminatingLocalConcurrency(updateProducers)
                .thenCompose(this::awaitVersionActivation)
                .whenComplete((result, err) -> {
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
                        resultFuture.complete(result);
                    }
                });

        return resultFuture;
    }

    private CompletableFuture<CatalogApplyResult> saveUpdateEliminatingLocalConcurrency(List<UpdateProducer> updateProducer) {
        // Avoid useless and wasteful competition for the save catalog version by enforcing an order.
        synchronized (lastSaveUpdateFutureMutex) {
            CompletableFuture<CatalogApplyResult> chainedFuture = lastSaveUpdateFuture
                    .thenCompose(unused -> saveUpdate(updateProducer, 0));

            // Suppressing any exception to make sure it doesn't ruin subsequent appends. The suppression is not a problem
            // as the callers will handle exceptions anyway.
            lastSaveUpdateFuture = chainedFuture.handle((res, ex) -> null);

            return chainedFuture;
        }
    }

    private CompletableFuture<CatalogApplyResult> awaitVersionActivation(CatalogApplyResult catalogApplyResult) {
        return awaitVersionActivation(catalogApplyResult.getCatalogVersion()).thenApply(unused -> catalogApplyResult);
    }

    private CompletableFuture<Integer> awaitVersionActivation(int version) {
        Catalog catalog = catalogByVer.get(version);

        HybridTimestamp tsSafeForRoReadingInPastOptimization = calcClusterWideEnsureActivationTime(catalog);

        return clockService.waitFor(tsSafeForRoReadingInPastOptimization).thenApply(unused -> version);
    }

    private HybridTimestamp calcClusterWideEnsureActivationTime(Catalog catalog) {
        return clusterWideEnsuredActivationTimestamp(catalog.time(), clockService.maxClockSkewMillis());
    }

    /**
     * Attempts to save a versioned update using a CAS-like logic. If the attempt fails, makes more attempts until the max retry count is
     * reached.
     *
     * @param updateProducers List of simple updates to include into a versioned update to install.
     * @param attemptNo Ordinal number of an attempt.
     * @return Future that completes with the result of applying updates, contains the Catalog version when the updates are visible or an
     *         exception in case of any error.
     */
    private CompletableFuture<CatalogApplyResult> saveUpdate(List<UpdateProducer> updateProducers, int attemptNo) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            if (attemptNo >= MAX_RETRY_COUNT) {
                return failedFuture(new IgniteInternalException(INTERNAL_ERR, "Max retry limit exceeded: " + attemptNo));
            }

            Catalog catalog = catalogByVer.lastValue();

            BitSet applyResults = new BitSet(updateProducers.size());
            List<UpdateEntry> bulkUpdateEntries = new ArrayList<>();
            try {
                UpdateContext updateContext = new UpdateContext(catalog);
                for (int i = 0; i < updateProducers.size(); i++) {
                    UpdateProducer update = updateProducers.get(i);
                    List<UpdateEntry> entries = update.get(updateContext);

                    if (entries.isEmpty()) {
                        continue;
                    }

                    for (UpdateEntry entry : entries) {
                        updateContext.updateCatalog(cat -> entry.applyUpdate(cat, INITIAL_TIMESTAMP));
                    }

                    applyResults.set(i);
                    bulkUpdateEntries.addAll(entries);
                }
            } catch (CatalogValidationException ex) {
                return failedFuture(new CatalogVersionAwareValidationException(ex, catalog.version()));
            } catch (Exception ex) {
                return failedFuture(ex);
            }

            // all results are empty.
            if (applyResults.cardinality() == 0) {
                return completedFuture(new CatalogApplyResult(applyResults, catalog.version(), catalog.time()));
            }

            int newVersion = catalog.version() + 1;

            // It is quite important to preserve such behavior: we wait here for versionTracker to be updated. It is updated when all events
            // that were triggered by this change will be completed. That means that any Catalog update will be completed only
            // after all reactions to that event will be completed through the catalog event notifications mechanism.
            // This is important for the distribution zones recovery purposes:
            // we guarantee recovery for a zones' catalog actions only if that actions were completed.
            return updateLog.append(new VersionedUpdate(newVersion, delayDurationMsSupplier.getAsLong(), bulkUpdateEntries))
                    .thenCompose(result -> {
                        return inBusyLockAsync(busyLock, () -> versionTracker.waitFor(newVersion).thenApply(none -> result));
                    })
                    .thenCompose(result -> {
                        if (result) {
                            long newCatalogTime = catalogByVer.get(newVersion).time();
                            return completedFuture(new CatalogApplyResult(applyResults, newVersion, newCatalogTime));
                        }

                        return saveUpdate(updateProducers, attemptNo + 1);
                    });
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public List<SystemView<?>> systemViews() {
        return catalogSystemViewProvider.systemViews();
    }

    private class OnUpdateHandlerImpl implements OnUpdateHandler {
        @Override
        public CompletableFuture<Void> handle(UpdateLogEvent event, HybridTimestamp metaStorageUpdateTimestamp, long causalityToken) {
            if (event instanceof SnapshotEntry) {
                return handle((SnapshotEntry) event);
            }

            return handle((VersionedUpdate) event, metaStorageUpdateTimestamp, causalityToken);
        }

        private CompletableFuture<Void> handle(SnapshotEntry event) {
            Catalog catalog = upgradeCatalog(event.snapshot());

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
                catalog = entry.applyUpdate(catalog, metaStorageUpdateTimestamp);
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
                        if (err != null && !hasCause(err, NodeStoppingException.class)) {
                            failureProcessor.process(new FailureContext(err, "Failed to apply catalog update."));
                        }

                        if (!busyLock.enterBusy()) {
                            return;
                        }

                        try {
                            versionTracker.update(version, null);
                        } finally {
                            busyLock.leaveBusy();
                        }
                    });
        }
    }

    private static Catalog upgradeCatalog(Catalog snapshot) {
        List<CatalogSchemaDescriptor> upgradedSchemas = new ArrayList<>(snapshot.schemas().size());
        for (CatalogSchemaDescriptor schema : snapshot.schemas()) {
            CatalogIndexDescriptor[] upgradedIndexes = upgradeIndexes(schema.tables(), schema.indexes());

            upgradedSchemas.add(new CatalogSchemaDescriptor(
                    schema.id(), schema.name(), schema.tables(), upgradedIndexes, schema.systemViews(), schema.updateTimestamp()
            ));
        }

        return new Catalog(
                snapshot.version(),
                snapshot.time(),
                snapshot.objectIdGenState(),
                snapshot.zones(),
                upgradedSchemas,
                defaultZoneIdOpt(snapshot)
        );
    }

    private static CatalogIndexDescriptor[] upgradeIndexes(
            CatalogTableDescriptor[] tables,
            CatalogIndexDescriptor[] indexes
    ) {
        Int2ObjectMap<CatalogTableDescriptor> tablesById = new Int2ObjectOpenHashMap<>();
        for (CatalogTableDescriptor table : tables) {
            tablesById.put(table.id(), table);
        }

        CatalogIndexDescriptor[] upgradedIndexes = new CatalogIndexDescriptor[indexes.length];
        for (int i = 0; i < indexes.length; i++) {
            CatalogIndexDescriptor index = indexes[i];
            CatalogTableDescriptor table = tablesById.get(index.tableId());

            assert table != null;

            upgradedIndexes[i] = index.upgradeIfNeeded(table);
        }

        return upgradedIndexes;
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

}
