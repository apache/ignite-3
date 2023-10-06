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
import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.validateAlterZoneParams;
import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.validateCreateZoneParams;
import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.validateDropZoneParams;
import static org.apache.ignite.internal.catalog.CatalogParamsValidationUtils.validateRenameZoneParams;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.fromParams;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.fromParamsAndPreviousValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.LongSupplier;
import org.apache.ignite.internal.catalog.commands.AlterZoneParams;
import org.apache.ignite.internal.catalog.commands.CreateZoneParams;
import org.apache.ignite.internal.catalog.commands.DropZoneParams;
import org.apache.ignite.internal.catalog.commands.RenameZoneParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSystemViewDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.storage.AlterZoneEntry;
import org.apache.ignite.internal.catalog.storage.DropZoneEntry;
import org.apache.ignite.internal.catalog.storage.Fireable;
import org.apache.ignite.internal.catalog.storage.NewZoneEntry;
import org.apache.ignite.internal.catalog.storage.ObjectIdGenUpdateEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;
import org.apache.ignite.internal.catalog.storage.UpdateLog;
import org.apache.ignite.internal.catalog.storage.UpdateLog.OnUpdateHandler;
import org.apache.ignite.internal.catalog.storage.VersionedUpdate;
import org.apache.ignite.internal.distributionzones.DistributionZoneAlreadyExistsException;
import org.apache.ignite.internal.distributionzones.DistributionZoneBindTableException;
import org.apache.ignite.internal.distributionzones.DistributionZoneNotFoundException;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.ErrorGroups.DistributionZones;
import org.jetbrains.annotations.Nullable;

/**
 * Catalog service implementation.
 */
public class CatalogManagerImpl extends AbstractEventProducer<CatalogEvent, CatalogEventParameters> implements CatalogManager {
    private static final int MAX_RETRY_COUNT = 10;

    /** Safe time to wait before new Catalog version activation. */
    private static final int DEFAULT_DELAY_DURATION = 0;

    /** Initial update token for a catalog descriptor, this token is valid only before the first call of
     * {@link UpdateEntry#applyUpdate(org.apache.ignite.internal.catalog.Catalog, long)}.
     * After that {@link CatalogObjectDescriptor#updateToken()} will be initialised with a causality token from
     * {@link UpdateEntry#applyUpdate(org.apache.ignite.internal.catalog.Catalog, long)}
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

    /**
     * Constructor.
     */
    public CatalogManagerImpl(UpdateLog updateLog, ClockWaiter clockWaiter) {
        this(updateLog, clockWaiter, DEFAULT_DELAY_DURATION);
    }

    /**
     * Constructor.
     */
    CatalogManagerImpl(UpdateLog updateLog, ClockWaiter clockWaiter, long delayDurationMs) {
        this(updateLog, clockWaiter, () -> delayDurationMs);
    }

    /**
     * Constructor.
     */
    public CatalogManagerImpl(UpdateLog updateLog, ClockWaiter clockWaiter, LongSupplier delayDurationMsSupplier) {
        this.updateLog = updateLog;
        this.clockWaiter = clockWaiter;
        this.delayDurationMsSupplier = delayDurationMsSupplier;
    }

    @Override
    public void start() {
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
                CreateZoneParams.builder().zoneName(DEFAULT_ZONE_NAME).build()
        );

        registerCatalog(new Catalog(0, 0L, objectIdGen, List.of(defaultZone), List.of(publicSchema, systemSchema)));

        updateLog.registerUpdateHandler(new OnUpdateHandlerImpl());

        updateLog.start();
    }

    @Override
    public void stop() throws Exception {
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
    public @Nullable CatalogIndexDescriptor index(String indexName, long timestamp) {
        return catalogAt(timestamp).schema(DEFAULT_SCHEMA_NAME).index(indexName);
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
    public @Nullable CatalogSchemaDescriptor schema(int version) {
        Catalog catalog = catalog(version);

        if (catalog == null) {
            return null;
        }

        return catalog.schema(DEFAULT_SCHEMA_NAME);
    }

    @Override
    public @Nullable CatalogSchemaDescriptor schema(String schemaName, int version) {
        Catalog catalog = catalog(version);

        if (catalog == null) {
            return null;
        }

        return catalog.schema(schemaName == null ? DEFAULT_SCHEMA_NAME : schemaName);
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
    public int latestCatalogVersion() {
        return catalogByVer.lastEntry().getKey();
    }

    @Override
    public CompletableFuture<Void> catalogReadyFuture(int version) {
        return versionTracker.waitFor(version);
    }

    private Catalog catalog(int version) {
        return catalogByVer.get(version);
    }

    private Catalog catalogAt(long timestamp) {
        Entry<Long, Catalog> entry = catalogByTs.floorEntry(timestamp);

        if (entry == null) {
            throw new IllegalStateException("No valid schema found for given timestamp: " + timestamp);
        }

        return entry.getValue();
    }

    @Override
    public CompletableFuture<Void> execute(CatalogCommand command) {
        return saveUpdateAndWaitForActivation(command);
    }

    @Override
    public CompletableFuture<Void> execute(List<CatalogCommand> commands) throws IllegalArgumentException {
        return saveUpdateAndWaitForActivation(new BulkUpdateProducer(List.copyOf(commands)));
    }

    @Override
    public CompletableFuture<Void> createZone(CreateZoneParams params) {
        return saveUpdateAndWaitForActivation(catalog -> {
            validateCreateZoneParams(params);

            if (catalog.zone(params.zoneName()) != null) {
                throw new DistributionZoneAlreadyExistsException(params.zoneName());
            }

            CatalogZoneDescriptor zone = fromParams(catalog.objectIdGenState(), params);

            return List.of(
                    new NewZoneEntry(zone),
                    new ObjectIdGenUpdateEntry(1)
            );
        });
    }

    @Override
    public CompletableFuture<Void> dropZone(DropZoneParams params) {
        return saveUpdateAndWaitForActivation(catalog -> {
            validateDropZoneParams(params);

            CatalogZoneDescriptor zone = getZone(catalog, params.zoneName());

            if (zone.name().equals(DEFAULT_ZONE_NAME)) {
                throw new IgniteInternalException(
                        DistributionZones.ZONE_DROP_ERR,
                        "Default distribution zone can't be dropped"
                );
            }

            catalog.schemas().stream()
                    .flatMap(s -> Arrays.stream(s.tables()))
                    .filter(t -> t.zoneId() == zone.id())
                    .findAny()
                    .ifPresent(t -> {
                        throw new DistributionZoneBindTableException(zone.name(), t.name());
                    });

            return List.of(new DropZoneEntry(zone.id()));
        });
    }

    @Override
    public CompletableFuture<Void> renameZone(RenameZoneParams params) {
        return saveUpdateAndWaitForActivation(catalog -> {
            validateRenameZoneParams(params);

            CatalogZoneDescriptor zone = getZone(catalog, params.zoneName());

            if (catalog.zone(params.newZoneName()) != null) {
                throw new DistributionZoneAlreadyExistsException(params.newZoneName());
            }

            if (zone.name().equals(DEFAULT_ZONE_NAME)) {
                throw new IgniteInternalException(
                        DistributionZones.ZONE_RENAME_ERR,
                        "Default distribution zone can't be renamed"
                );
            }

            CatalogZoneDescriptor descriptor = new CatalogZoneDescriptor(
                    zone.id(),
                    params.newZoneName(),
                    zone.partitions(),
                    zone.replicas(),
                    zone.dataNodesAutoAdjust(),
                    zone.dataNodesAutoAdjustScaleUp(),
                    zone.dataNodesAutoAdjustScaleDown(),
                    zone.filter(),
                    zone.dataStorage()
            );

            return List.of(new AlterZoneEntry(descriptor));
        });
    }

    @Override
    public CompletableFuture<Void> alterZone(AlterZoneParams params) {
        return saveUpdateAndWaitForActivation(catalog -> {
            validateAlterZoneParams(params);

            CatalogZoneDescriptor zone = getZone(catalog, params.zoneName());

            CatalogZoneDescriptor descriptor = fromParamsAndPreviousValue(params, zone);

            return List.of(new AlterZoneEntry(descriptor));
        });
    }

    private void registerCatalog(Catalog newCatalog) {
        catalogByVer.put(newCatalog.version(), newCatalog);
        catalogByTs.put(newCatalog.time(), newCatalog);
    }

    private CompletableFuture<Void> saveUpdateAndWaitForActivation(UpdateProducer updateProducer) {
        return saveUpdate(updateProducer, 0)
                .thenCompose(newVersion -> {
                    Catalog catalog = catalogByVer.get(newVersion);

                    HybridTimestamp activationTs = HybridTimestamp.hybridTimestamp(catalog.time());
                    HybridTimestamp clusterWideEnsuredActivationTs = activationTs.addPhysicalTime(
                            HybridTimestamp.maxClockSkew()
                    );

                    return clockWaiter.waitFor(clusterWideEnsuredActivationTs);
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

        return updateLog.append(new VersionedUpdate(newVersion, delayDurationMsSupplier.getAsLong(), updates))
                .thenCompose(result -> versionTracker.waitFor(newVersion).thenApply(none -> result))
                .thenCompose(result -> {
                    if (result) {
                        return completedFuture(newVersion);
                    }

                    return saveUpdate(updateProducer, attemptNo + 1);
                });
    }

    class OnUpdateHandlerImpl implements OnUpdateHandler {
        @Override
        public CompletableFuture<Void> handle(VersionedUpdate update, HybridTimestamp metaStorageUpdateTimestamp, long causalityToken) {
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

            return allOf(eventFutures.toArray(CompletableFuture[]::new))
                    .whenComplete((ignore, err) -> {
                        if (err != null) {
                            LOG.warn("Failed to apply catalog update.", err);
                            //TODO: IGNITE-14611 Pass exception to an error handler because catalog got into inconsistent state.
                        }

                        versionTracker.update(version, null);
                    });
        }
    }

    private static Catalog applyUpdateFinal(Catalog catalog, VersionedUpdate update, HybridTimestamp metaStorageUpdateTimestamp) {
        long activationTimestamp = metaStorageUpdateTimestamp.addPhysicalTime(update.delayDurationMs()).longValue();

        return new Catalog(
                update.version(),
                activationTimestamp,
                catalog.objectIdGenState(),
                catalog.zones(),
                catalog.schemas()
        );
    }

    private static CatalogZoneDescriptor getZone(Catalog catalog, String zoneName) {
        zoneName = Objects.requireNonNull(zoneName, "zoneName");

        CatalogZoneDescriptor zone = catalog.zone(zoneName);

        if (zone == null) {
            throw new DistributionZoneNotFoundException(zoneName);
        }

        return zone;
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
