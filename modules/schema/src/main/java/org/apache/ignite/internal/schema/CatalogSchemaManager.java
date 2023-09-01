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

package org.apache.ignite.internal.schema;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNullElse;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.util.ByteUtils.intToBytes;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import java.util.stream.IntStream;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.CreateTableEventParameters;
import org.apache.ignite.internal.catalog.events.TableEventParameters;
import org.apache.ignite.internal.causality.IncrementalVersionedValue;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.manager.Producer;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.schema.event.SchemaEvent;
import org.apache.ignite.internal.schema.event.SchemaEventParameters;
import org.apache.ignite.internal.schema.marshaller.schema.SchemaSerializerImpl;
import org.apache.ignite.internal.schema.registry.SchemaRegistryImpl;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.Nullable;

/**
 * This class services management of table schemas.
 */
public class CatalogSchemaManager extends Producer<SchemaEvent, SchemaEventParameters> implements IgniteComponent {
    private static final IgniteLogger LOGGER = Loggers.forClass(CatalogSchemaManager.class);

    /** Schema history key predicate part. */
    private static final String SCHEMA_STORE_PREFIX = ".sch-hist.";
    private static final String LATEST_SCHEMA_VERSION_STORE_SUFFIX = ".sch-hist-latest";

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    private final CatalogService catalogService;

    /** Versioned store for tables by ID. */
    private final IncrementalVersionedValue<Map<Integer, SchemaRegistryImpl>> registriesVv;

    /** Meta storage manager. */
    private final MetaStorageManager metastorageMgr;

    /** Constructor. */
    public CatalogSchemaManager(
            Consumer<LongFunction<CompletableFuture<?>>> registry,
            CatalogService catalogService,
            MetaStorageManager metastorageMgr
    ) {
        this.registriesVv = new IncrementalVersionedValue<>(registry, HashMap::new);
        this.catalogService = catalogService;
        this.metastorageMgr = metastorageMgr;
    }

    @Override
    public void start() {
        catalogService.listen(CatalogEvent.TABLE_CREATE, this::onTableCreated);
        catalogService.listen(CatalogEvent.TABLE_ALTER, this::onTableAltered);

        registerExistingTables();
    }

    @WithSpan
    private void registerExistingTables() {
        // TODO: IGNITE-20051 - add proper recovery (consider tables that are removed now; take token and catalog version
        // exactly matching the tables).

        long causalityToken = metastorageMgr.appliedRevision();
        int catalogVersion = catalogService.latestCatalogVersion();

        for (CatalogTableDescriptor tableDescriptor : catalogService.tables(catalogVersion)) {
            onTableCreated(new CreateTableEventParameters(causalityToken, catalogVersion, tableDescriptor), null);
        }

        registriesVv.complete(causalityToken);
    }

    @WithSpan
    private CompletableFuture<Boolean> onTableCreated(CatalogEventParameters event, @Nullable Throwable ex) {
        if (ex != null) {
            return failedFuture(ex);
        }

        CreateTableEventParameters creationEvent = (CreateTableEventParameters) event;

        return onTableCreatedOrAltered(creationEvent.tableDescriptor(), creationEvent.causalityToken());
    }

    @WithSpan
    private CompletableFuture<Boolean> onTableAltered(CatalogEventParameters event, @Nullable Throwable ex) {
        if (ex != null) {
            return failedFuture(ex);
        }

        assert event instanceof TableEventParameters;

        TableEventParameters tableEvent = ((TableEventParameters) event);

        CatalogTableDescriptor tableDescriptor = catalogService.table(tableEvent.tableId(), tableEvent.catalogVersion());

        assert tableDescriptor != null;

        return onTableCreatedOrAltered(tableDescriptor, event.causalityToken());
    }

    @WithSpan
    private CompletableFuture<Boolean> onTableCreatedOrAltered(CatalogTableDescriptor tableDescriptor, long causalityToken) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            int tableId = tableDescriptor.id();
            int newSchemaVersion = tableDescriptor.tableVersion();

            if (searchSchemaByVersion(tableId, newSchemaVersion) != null) {
                return completedFuture(null);
            }

            SchemaDescriptor newSchema = SchemaUtils.prepareSchemaDescriptor(tableDescriptor);

            // This is intentionally a blocking call to enforce catalog listener execution order. Unfortunately it is not possible
            // to execute this method asynchronously, because the schema descriptor is needed to fire the CREATE event as a synchronous part
            // of the catalog listener.
            try {
                setColumnMapping(newSchema, tableId);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                return failedFuture(e);
            } catch (ExecutionException e) {
                return failedFuture(e);
            }

            // Fire event early, because dependent listeners have to register VersionedValues' update futures
            var eventParams = new SchemaEventParameters(causalityToken, tableId, newSchema);

            fireEvent(SchemaEvent.CREATE, eventParams)
                    .whenComplete((v, e) -> {
                        if (e != null) {
                            LOGGER.warn("Error when processing CREATE event", e);
                        }
                    });

            return registriesVv.update(causalityToken, (registries, e) -> inBusyLock(busyLock, () -> {
                if (e != null) {
                    return failedFuture(new IgniteInternalException(IgniteStringFormatter.format(
                            "Cannot create a schema for the table [tblId={}, ver={}]", tableId, newSchemaVersion), e)
                    );
                }

                return saveSchemaDescriptor(tableId, newSchema)
                        .thenApply(t -> registerSchema(registries, tableId, newSchema));
            })).thenApply(ignored -> false);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private void setColumnMapping(SchemaDescriptor schema, int tableId) throws ExecutionException, InterruptedException {
        if (schema.version() == CatalogTableDescriptor.INITIAL_TABLE_VERSION) {
            return;
        }

        int prevVersion = schema.version() - 1;

        SchemaDescriptor prevSchema = searchSchemaByVersion(tableId, prevVersion);

        if (prevSchema == null) {
            // This is intentionally a blocking call, because this method is used in a synchronous part of the configuration listener.
            // See the call site for more details.
            prevSchema = loadSchemaDescriptor(tableId, prevVersion).get();
        }

        schema.columnMapping(SchemaUtils.columnMapper(prevSchema, schema));
    }

    /**
     * Loads the table schema descriptor by version from Metastore.
     *
     * @param tblId Table id.
     * @param ver Schema version.
     * @return Schema representation if schema found, {@code null} otherwise.
     */
    private CompletableFuture<SchemaDescriptor> loadSchemaDescriptor(int tblId, int ver) {
        return metastorageMgr.get(schemaWithVerHistKey(tblId, ver))
                .thenApply(entry -> {
                    byte[] value = entry.value();

                    assert value != null;

                    return SchemaSerializerImpl.INSTANCE.deserialize(value);
                });
    }

    /**
     * Saves a schema in the MetaStorage.
     *
     * @param tableId Table id.
     * @param schema Schema descriptor.
     * @return Future that will be completed when the schema gets saved.
     */
    @WithSpan
    private CompletableFuture<Void> saveSchemaDescriptor(int tableId, SchemaDescriptor schema) {
        ByteArray schemaKey = schemaWithVerHistKey(tableId, schema.version());
        ByteArray latestSchemaVersionKey = latestSchemaVersionKey(tableId);

        byte[] serializedSchema = SchemaSerializerImpl.INSTANCE.serialize(schema);

        return metastorageMgr.invoke(
                notExists(schemaKey),
                List.of(
                        put(schemaKey, serializedSchema),
                        put(latestSchemaVersionKey, intToBytes(schema.version()))
                ),
                emptyList()
        ).thenApply(unused -> null);
    }

    /**
     * Registers the new schema in the registries.
     *
     * @param registries Registries before registering this schema.
     * @param tableId ID of the table to which the schema belongs.
     * @param schema The schema to register.
     * @return Registries after registering this schema.
     */
    @WithSpan
    private Map<Integer, SchemaRegistryImpl> registerSchema(
            Map<Integer, SchemaRegistryImpl> registries,
            int tableId,
            SchemaDescriptor schema
    ) {
        SchemaRegistryImpl reg = registries.get(tableId);

        if (reg == null) {
            Map<Integer, SchemaRegistryImpl> copy = new HashMap<>(registries);

            copy.put(tableId, createSchemaRegistry(tableId, schema));

            return copy;
        } else {
            reg.onSchemaRegistered(schema);

            return registries;
        }
    }

    /**
     * Create schema registry for the table.
     *
     * @param tableId Table id.
     * @param initialSchema Initial schema for the registry.
     * @return Schema registry.
     */
    private SchemaRegistryImpl createSchemaRegistry(int tableId, SchemaDescriptor initialSchema) {
        return new SchemaRegistryImpl(
                ver -> inBusyLock(busyLock, () -> loadSchemaDescriptor(tableId, ver)),
                () -> inBusyLock(busyLock, () -> latestSchemaVersionOrDefault(tableId)),
                initialSchema
        );
    }

    /**
     * Try to find schema in cache.
     *
     * @param tblId Table id.
     * @param schemaVer Schema version.
     * @return Descriptor if required schema found, or {@code null} otherwise.
     */
    private @Nullable SchemaDescriptor searchSchemaByVersion(int tblId, int schemaVer) {
        SchemaRegistry registry = registriesVv.latest().get(tblId);

        if (registry != null && schemaVer <= registry.lastSchemaVersion()) {
            return registry.schema(schemaVer);
        } else {
            return null;
        }
    }

    /**
     * Get the schema registry for the given causality token and table id.
     *
     * @param causalityToken Causality token.
     * @param tableId Id of a table which the required registry belongs to. If {@code null}, then this method will return a future which
     *     will be completed with {@code null} result, but only when the schema manager will have consistent state regarding given causality
     *     token.
     * @return A future which will be completed when schema registries for given causality token are ready.
     */
    public CompletableFuture<SchemaRegistry> schemaRegistry(long causalityToken, int tableId) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            return registriesVv.get(causalityToken)
                    .thenApply(regs -> inBusyLock(busyLock, () -> regs.get(tableId)));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Returns schema registry by table id.
     *
     * @param tableId Table id.
     * @return Schema registry.
     */
    public SchemaRegistry schemaRegistry(int tableId) {
        return registriesVv.latest().get(tableId);
    }

    /**
     * Drops schema registry for the given table id (along with the corresponding schemas).
     *
     * @param causalityToken Causality token.
     * @param tableId Table id.
     */
    public CompletableFuture<?> dropRegistry(long causalityToken, int tableId) {
        return removeRegistry(causalityToken, tableId).thenCompose(unused -> {
            return destroySchemas(tableId);
        });
    }

    private CompletableFuture<?> removeRegistry(long causalityToken, int tableId) {
        return registriesVv.update(causalityToken, (registries, e) -> inBusyLock(busyLock, () -> {
            if (e != null) {
                return failedFuture(new IgniteInternalException(
                        IgniteStringFormatter.format("Cannot remove a schema registry for the table [tblId={}]", tableId), e));
            }

            Map<Integer, SchemaRegistryImpl> regs = new HashMap<>(registries);

            regs.remove(tableId);

            return completedFuture(regs);
        }));
    }

    private CompletableFuture<?> destroySchemas(int tableId) {
        return latestSchemaVersion(tableId)
                .thenCompose(latestVersion -> {
                    if (latestVersion == null) {
                        // Nothing to remove.
                        return completedFuture(null);
                    }

                    Set<ByteArray> keysToRemove = IntStream.rangeClosed(CatalogTableDescriptor.INITIAL_TABLE_VERSION, latestVersion)
                            .mapToObj(version -> schemaWithVerHistKey(tableId, version))
                            .collect(toSet());
                    keysToRemove.add(latestSchemaVersionKey(tableId));

                    return metastorageMgr.removeAll(keysToRemove);
                });
    }

    @Override
    public void stop() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();
    }

    /**
     * Gets the latest version of the table schema which is available in Metastore or the default (1) if nothing is available.
     *
     * @param tableId Table id.
     * @return The latest schema version.
     */
    private CompletableFuture<Integer> latestSchemaVersionOrDefault(int tableId) {
        return latestSchemaVersion(tableId)
                .thenApply(versionOrNull -> requireNonNullElse(versionOrNull, CatalogTableDescriptor.INITIAL_TABLE_VERSION));
    }

    /**
     * Gets the latest version of the table schema which is available in Metastore or {@code null} if nothing is available.
     *
     * @param tableId Table id.
     * @return The latest schema version or {@code null} if nothing is available.
     */
    private CompletableFuture<Integer> latestSchemaVersion(int tableId) {
        return metastorageMgr.get(latestSchemaVersionKey(tableId))
                .thenApply(entry -> {
                    if (entry == null || entry.value() == null) {
                        return null;
                    } else {
                        return ByteUtils.bytesToInt(entry.value());
                    }
                });
    }

    /**
     * Forms schema history key.
     *
     * @param tblId Table id.
     * @param ver Schema version.
     * @return {@link ByteArray} representation.
     */
    private static ByteArray schemaWithVerHistKey(int tblId, int ver) {
        return ByteArray.fromString(tblId + SCHEMA_STORE_PREFIX + ver);
    }

    private static ByteArray latestSchemaVersionKey(int tableId) {
        return ByteArray.fromString(tableId + LATEST_SCHEMA_VERSION_STORE_SUFFIX);
    }
}
