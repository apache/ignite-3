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
import static java.util.stream.Collectors.joining;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.clusterWideEnsuredActivationTsSafeForRoReads;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.fromParams;
import static org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor.CatalogIndexDescriptorType.HASH;
import static org.apache.ignite.internal.type.NativeTypes.BOOLEAN;
import static org.apache.ignite.internal.type.NativeTypes.INT32;
import static org.apache.ignite.internal.type.NativeTypes.STRING;
import static org.apache.ignite.internal.type.NativeTypes.stringOf;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Flow.Publisher;
import java.util.function.Function;
import java.util.function.LongSupplier;
import org.apache.ignite.internal.catalog.descriptors.CatalogHashIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSortedIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
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
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViewProvider;
import org.apache.ignite.internal.systemview.api.SystemViews;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.util.SubscriptionUtils;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.jetbrains.annotations.Nullable;

/**
 * Catalog service implementation.
 */
public class CatalogManagerImpl extends AbstractEventProducer<CatalogEvent, CatalogEventParameters>
        implements CatalogManager, SystemViewProvider {
    private static final int MAX_RETRY_COUNT = 10;

    private static final int SYSTEM_VIEW_STRING_COLUMN_LENGTH = Short.MAX_VALUE;

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

    private final UpdateLog updateLog;

    private final PendingComparableValuesTracker<Integer, Void> versionTracker = new PendingComparableValuesTracker<>(0);

    private final ClockWaiter clockWaiter;

    private final LongSupplier delayDurationMsSupplier;

    private final LongSupplier partitionIdleSafeTimePropagationPeriodMsSupplier;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Clock. */
    private final HybridClock clock;

    /**
     * Constructor.
     */
    public CatalogManagerImpl(UpdateLog updateLog, ClockWaiter clockWaiter, HybridClock clock) {
        this(updateLog, clockWaiter, clock, DEFAULT_DELAY_DURATION, DEFAULT_PARTITION_IDLE_SAFE_TIME_PROPAGATION_PERIOD);
    }

    /**
     * Constructor.
     */
    CatalogManagerImpl(UpdateLog updateLog,
            ClockWaiter clockWaiter,
            HybridClock clock,
            long delayDurationMs,
            long partitionIdleSafeTimePropagationPeriod
    ) {
        this(updateLog, clockWaiter, clock, () -> delayDurationMs, () -> partitionIdleSafeTimePropagationPeriod);
    }

    /**
     * Constructor.
     */
    public CatalogManagerImpl(
            UpdateLog updateLog,
            ClockWaiter clockWaiter,
            HybridClock clock,
            LongSupplier delayDurationMsSupplier,
            LongSupplier partitionIdleSafeTimePropagationPeriodMsSupplier
    ) {
        this.updateLog = updateLog;
        this.clockWaiter = clockWaiter;
        this.clock = clock;
        this.delayDurationMsSupplier = delayDurationMsSupplier;
        this.partitionIdleSafeTimePropagationPeriodMsSupplier = partitionIdleSafeTimePropagationPeriodMsSupplier;
    }

    @Override
    public CompletableFuture<Void> start() {
        int objectIdGen = 0;

        // TODO: IGNITE-19082 Move default schema objects initialization to cluster init procedure.
        CatalogSchemaDescriptor publicSchema = new CatalogSchemaDescriptor(
                objectIdGen++,
                DEFAULT_SCHEMA_NAME,
                new CatalogTableDescriptor[0],
                new CatalogIndexDescriptor[0],
                new CatalogSystemViewDescriptor[0],
                INITIAL_CAUSALITY_TOKEN
        );

        // TODO: IGNITE-19082 Move system schema objects initialization to cluster init procedure.
        CatalogSchemaDescriptor systemSchema = new CatalogSchemaDescriptor(
                objectIdGen++,
                SYSTEM_SCHEMA_NAME,
                new CatalogTableDescriptor[0],
                new CatalogIndexDescriptor[0],
                new CatalogSystemViewDescriptor[0],
                INITIAL_CAUSALITY_TOKEN
        );

        CatalogZoneDescriptor defaultZone = fromParams(
                objectIdGen++,
                DEFAULT_ZONE_NAME
        );

        registerCatalog(new Catalog(0, 0L, objectIdGen, List.of(defaultZone), List.of(publicSchema, systemSchema)));

        updateLog.registerUpdateHandler(new OnUpdateHandlerImpl());

        updateLog.start();

        return nullCompletedFuture();
    }

    @Override
    public void stop() throws Exception {
        busyLock.block();
        versionTracker.close();
        updateLog.stop();
    }

    @Override
    public @Nullable CatalogTableDescriptor table(String tableName, long timestamp) {
        return catalogAt(timestamp).schema(DEFAULT_SCHEMA_NAME).table(tableName);
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
        return catalogAt(timestamp).schema(DEFAULT_SCHEMA_NAME).aliveIndex(indexName);
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
        return schema(DEFAULT_SCHEMA_NAME, catalogVersion);
    }

    @Override
    public @Nullable CatalogSchemaDescriptor schema(String schemaName, int catalogVersion) {
        Catalog catalog = catalog(catalogVersion);

        if (catalog == null) {
            return null;
        }

        return catalog.schema(schemaName == null ? DEFAULT_SCHEMA_NAME : schemaName);
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
        return catalogAt(timestamp).schema(DEFAULT_SCHEMA_NAME);
    }

    @Override
    public @Nullable CatalogSchemaDescriptor activeSchema(String schemaName, long timestamp) {
        return catalogAt(timestamp).schema(schemaName == null ? DEFAULT_SCHEMA_NAME : schemaName);
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
        return saveUpdate(updateProducer, 0)
                .thenCompose(newVersion -> {
                    Catalog catalog = catalogByVer.get(newVersion);

                    HybridTimestamp tsSafeForRoReadingInPastOptimization = clusterWideEnsuredActivationTsSafeForRoReads(
                            catalog, partitionIdleSafeTimePropagationPeriodMsSupplier
                    );

                    return clockWaiter.waitFor(tsSafeForRoReadingInPastOptimization).thenApply(unused -> newVersion);
                });
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
        return List.of(
                createSystemViewsView(),
                createSystemViewColumnsView(),
                createZonesView(),
                createIndexesView()
        );
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
                catalog.schemas()
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

    private SystemView<?> createSystemViewsView() {
        Iterable<SchemaAwareDescriptor<CatalogSystemViewDescriptor>> viewData = () -> {
            Catalog catalog = catalogAt(clock.nowLong());

            return catalog.schemas().stream()
                    .flatMap(schema -> Arrays.stream(schema.systemViews())
                            .map(viewDescriptor -> new SchemaAwareDescriptor<>(viewDescriptor, schema.name()))
                    )
                    .iterator();
        };

        Publisher<SchemaAwareDescriptor<CatalogSystemViewDescriptor>> viewDataPublisher = SubscriptionUtils.fromIterable(viewData);

        return SystemViews.<SchemaAwareDescriptor<CatalogSystemViewDescriptor>>clusterViewBuilder()
                .name("SYSTEM_VIEWS")
                .addColumn("ID", INT32, entry -> entry.descriptor.id())
                .addColumn("SCHEMA", stringOf(SYSTEM_VIEW_STRING_COLUMN_LENGTH),
                        entry -> entry.schema)
                .addColumn("NAME", stringOf(SYSTEM_VIEW_STRING_COLUMN_LENGTH),
                        entry -> entry.descriptor.name())
                .addColumn("TYPE", stringOf(SYSTEM_VIEW_STRING_COLUMN_LENGTH),
                        entry -> entry.descriptor.systemViewType().name())
                .dataProvider(viewDataPublisher)
                .build();
    }

    private SystemView<?> createSystemViewColumnsView() {
        Iterable<ParentIdAwareDescriptor<CatalogTableColumnDescriptor>> viewData = () -> {
            Catalog catalog = catalogAt(clock.nowLong());

            return catalog.schemas().stream()
                    .flatMap(schema -> Arrays.stream(schema.systemViews()))
                    .flatMap(viewDescriptor -> viewDescriptor.columns().stream()
                            .map(columnDescriptor -> new ParentIdAwareDescriptor<>(columnDescriptor, viewDescriptor.id()))
                    )
                    .iterator();
        };

        Publisher<ParentIdAwareDescriptor<CatalogTableColumnDescriptor>> viewDataPublisher = SubscriptionUtils.fromIterable(viewData);

        return SystemViews.<ParentIdAwareDescriptor<CatalogTableColumnDescriptor>>clusterViewBuilder()
                .name("SYSTEM_VIEW_COLUMNS")
                .addColumn("VIEW_ID", INT32, entry -> entry.id)
                .addColumn("NAME", stringOf(SYSTEM_VIEW_STRING_COLUMN_LENGTH), entry -> entry.descriptor.name())
                .addColumn("TYPE", stringOf(SYSTEM_VIEW_STRING_COLUMN_LENGTH), entry -> entry.descriptor.type().name())
                .addColumn("NULLABLE", BOOLEAN, entry -> entry.descriptor.nullable())
                .addColumn("PRECISION", INT32, entry -> entry.descriptor.precision())
                .addColumn("SCALE", INT32, entry -> entry.descriptor.scale())
                .addColumn("LENGTH", INT32, entry -> entry.descriptor.length())
                .dataProvider(viewDataPublisher)
                .build();
    }

    private SystemView<?> createZonesView() {
        return SystemViews.<CatalogZoneDescriptor>clusterViewBuilder()
                .name("ZONES")
                .addColumn("NAME", STRING, CatalogZoneDescriptor::name)
                .addColumn("PARTITIONS", INT32, CatalogZoneDescriptor::partitions)
                .addColumn("REPLICAS", INT32, CatalogZoneDescriptor::replicas)
                .addColumn("DATA_NODES_AUTO_ADJUST_SCALE_UP", INT32, CatalogZoneDescriptor::dataNodesAutoAdjustScaleUp)
                .addColumn("DATA_NODES_AUTO_ADJUST_SCALE_DOWN", INT32, CatalogZoneDescriptor::dataNodesAutoAdjustScaleDown)
                .addColumn("DATA_NODES_FILTER", STRING, CatalogZoneDescriptor::filter)
                .addColumn("IS_DEFAULT_ZONE", BOOLEAN, isDefaultZone())
                .dataProvider(SubscriptionUtils.fromIterable(() -> catalogAt(clock.nowLong()).zones().iterator()))
                .build();
    }

    private SystemView<?> createIndexesView() {
        Iterable<CatalogAwareDescriptor<CatalogIndexDescriptor>> viewData = () -> {
            Catalog catalog = catalogAt(clock.nowLong());

            return catalog.indexes().stream()
                    .filter(index -> index.status().isAlive())
                    .map(index -> new CatalogAwareDescriptor<>(index, catalog))
                    .iterator();
        };

        return SystemViews.<CatalogAwareDescriptor<CatalogIndexDescriptor>>clusterViewBuilder()
                .name("INDEXES")
                .addColumn("INDEX_ID", INT32, entry -> entry.descriptor.id())
                .addColumn("INDEX_NAME", STRING, entry -> entry.descriptor.name())
                .addColumn("TABLE_ID", INT32, entry -> entry.descriptor.tableId())
                .addColumn("TABLE_NAME", STRING, entry -> getTableDescriptor(entry).name())
                .addColumn("SCHEMA_ID", INT32, CatalogManagerImpl::getSchemaId)
                .addColumn("SCHEMA_NAME", STRING, entry -> entry.catalog.schema(getSchemaId(entry)).name())
                .addColumn("TYPE", STRING, entry -> entry.descriptor.indexType().name())
                .addColumn("IS_UNIQUE", BOOLEAN, entry -> entry.descriptor.unique())
                .addColumn("COLUMNS", STRING, CatalogManagerImpl::getColumnsString)
                .addColumn("STATUS", STRING, entry -> entry.descriptor.status().name())
                .dataProvider(SubscriptionUtils.fromIterable(viewData))
                .build();
    }

    private static int getSchemaId(CatalogAwareDescriptor<CatalogIndexDescriptor> entry) {
        return getTableDescriptor(entry).schemaId();
    }

    private static CatalogTableDescriptor getTableDescriptor(CatalogAwareDescriptor<CatalogIndexDescriptor> entry) {
        return entry.catalog.table(entry.descriptor.tableId());
    }

    private static String getColumnsString(CatalogAwareDescriptor<CatalogIndexDescriptor> entry) {
        return entry.descriptor.indexType() == HASH
                ? String.join(", ", ((CatalogHashIndexDescriptor) entry.descriptor).columns())
                : ((CatalogSortedIndexDescriptor) entry.descriptor)
                        .columns()
                        .stream()
                        .map(column -> column.name() + (column.collation().asc() ? " ASC" : " DESC"))
                        .collect(joining(", "));
    }

    private static Function<CatalogZoneDescriptor, Boolean> isDefaultZone() {
        return zone -> zone.name().equals(DEFAULT_ZONE_NAME);
    }

    /**
     * A container that keeps given descriptor along with name of the schema this
     * descriptor belongs to.
     */
    private static class SchemaAwareDescriptor<T> {
        private final T descriptor;
        private final String schema;

        SchemaAwareDescriptor(T descriptor, String schema) {
            this.descriptor = descriptor;
            this.schema = schema;
        }
    }

    /**
     * A container that keeps given descriptor along with its parent's id.
     */
    private static class ParentIdAwareDescriptor<T> {
        private final T descriptor;
        private final int id;

        ParentIdAwareDescriptor(T descriptor, int id) {
            this.descriptor = descriptor;
            this.id = id;
        }
    }

    /**
     * A container that keeps given descriptor along with the catalog it belongs to.
     */
    private static class CatalogAwareDescriptor<T> {
        private final T descriptor;
        private final Catalog catalog;

        CatalogAwareDescriptor(T descriptor, Catalog catalog) {
            this.descriptor = descriptor;
            this.catalog = catalog;
        }
    }
}
