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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableSchemaVersions;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.CreateTableEventParameters;
import org.apache.ignite.internal.catalog.events.DestroyTableEventParameters;
import org.apache.ignite.internal.catalog.events.TableEventParameters;
import org.apache.ignite.internal.causality.IncrementalVersionedValue;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.schema.catalog.CatalogToSchemaDescriptorConverter;
import org.apache.ignite.internal.schema.registry.SchemaRegistryImpl;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteException;
import org.jetbrains.annotations.Nullable;

/**
 * This class services management of table schemas.
 */
public class SchemaManager implements IgniteComponent {
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
    public SchemaManager(
            Consumer<LongFunction<CompletableFuture<?>>> registry,
            CatalogService catalogService,
            MetaStorageManager metastorageMgr
    ) {
        this.registriesVv = new IncrementalVersionedValue<>(registry, HashMap::new);
        this.catalogService = catalogService;
        this.metastorageMgr = metastorageMgr;
    }

    @Override
    public CompletableFuture<Void> start() {
        catalogService.listen(CatalogEvent.TABLE_CREATE, this::onTableCreated);
        catalogService.listen(CatalogEvent.TABLE_ALTER, this::onTableAltered);
        catalogService.listen(CatalogEvent.TABLE_DESTROY, this::onTableDestroyed);

        registerExistingTables();

        return nullCompletedFuture();
    }

    private void registerExistingTables() {
        CompletableFuture<Long> recoveryFinishFuture = metastorageMgr.recoveryFinishedFuture();

        assert recoveryFinishFuture.isDone();

        long causalityToken = recoveryFinishFuture.join();

        for (int catalogVer = catalogService.latestCatalogVersion(); catalogVer >= catalogService.earliestCatalogVersion(); catalogVer--) {
            Collection<CatalogTableDescriptor> tables = catalogService.tables(catalogVer);

            registriesVv.update(causalityToken, (registries, throwable) -> {
                for (CatalogTableDescriptor tableDescriptor : tables) {
                    int tableId = tableDescriptor.id();

                    if (registries.containsKey(tableId)) {
                        continue;
                    }

                    SchemaDescriptor prevSchema = null;
                    CatalogTableSchemaVersions schemaVersions = tableDescriptor.schemaVersions();
                    for (int tableVer = schemaVersions.earliestVersion(); tableVer <= schemaVersions.latestVersion(); tableVer++) {
                        SchemaDescriptor newSchema = CatalogToSchemaDescriptorConverter.convert(tableDescriptor, tableVer);

                        if (prevSchema != null) {
                            newSchema.columnMapping(SchemaUtils.columnMapper(prevSchema, newSchema));
                        }

                        prevSchema = newSchema;
                        registries = registerSchema(registries, tableId, newSchema);
                    }
                }

                return completedFuture(registries);
            });
        }
    }

    private CompletableFuture<Boolean> onTableCreated(CatalogEventParameters event, @Nullable Throwable ex) {
        if (ex != null) {
            return failedFuture(ex);
        }

        CreateTableEventParameters creationEvent = (CreateTableEventParameters) event;

        return onTableCreatedOrAltered(creationEvent.tableDescriptor(), creationEvent.causalityToken());
    }

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

    private CompletableFuture<Boolean> onTableCreatedOrAltered(CatalogTableDescriptor tableDescriptor, long causalityToken) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            int tableId = tableDescriptor.id();
            int newSchemaVersion = tableDescriptor.tableVersion();

            if (searchSchemaByVersion(tableId, newSchemaVersion) != null) {
                return falseCompletedFuture();
            }

            SchemaDescriptor newSchema = SchemaUtils.prepareSchemaDescriptor(tableDescriptor);

            try {
                setColumnMapping(newSchema, tableId);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                return failedFuture(e);
            } catch (ExecutionException e) {
                return failedFuture(e);
            }

            return registriesVv.update(causalityToken, (registries, e) -> inBusyLock(busyLock, () -> {
                if (e != null) {
                    return failedFuture(new IgniteInternalException(format(
                            "Cannot create a schema for the table [tblId={}, ver={}]", tableId, newSchemaVersion), e)
                    );
                }

                return completedFuture(registerSchema(registries, tableId, newSchema));
            })).thenApply(ignored -> false);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private CompletableFuture<Boolean> onTableDestroyed(CatalogEventParameters event, @Nullable Throwable ex) {
        if (ex != null) {
            return failedFuture(ex);
        }

        DestroyTableEventParameters creationEvent = (DestroyTableEventParameters) event;

        return dropRegistry(creationEvent.causalityToken(), creationEvent.tableId()).thenApply(ignored -> false);
    }

    private void setColumnMapping(SchemaDescriptor schema, int tableId) throws ExecutionException, InterruptedException {
        if (schema.version() == CatalogTableDescriptor.INITIAL_TABLE_VERSION) {
            return;
        }

        int prevVersion = schema.version() - 1;

        SchemaDescriptor prevSchema = searchSchemaByVersion(tableId, prevVersion);

        if (prevSchema == null) {
            prevSchema = loadSchemaDescriptor(tableId, prevVersion);
        }

        schema.columnMapping(SchemaUtils.columnMapper(prevSchema, schema));
    }

    /**
     * Loads the table schema descriptor by version from local Metastore storage.
     * If called with a schema version for which the schema is not yet saved to the Metastore, an exception
     * will be thrown.
     *
     * @param tblId Table id.
     * @param ver Schema version (must not be higher than the latest version saved to the  Metastore).
     * @return Schema representation.
     */
    private SchemaDescriptor loadSchemaDescriptor(int tblId, int ver) {
        int catalogVersion = catalogService.latestCatalogVersion();

        while (catalogVersion >= catalogService.earliestCatalogVersion()) {
            CatalogTableDescriptor tableDescriptor = catalogService.table(tblId, catalogVersion);

            if (tableDescriptor == null) {
                catalogVersion--;

                continue;
            }

            return CatalogToSchemaDescriptorConverter.convert(tableDescriptor, ver);
        }

        throw new AssertionError(format("Schema descriptor is not found [tableId={}, schemaId={}]", tblId, ver));
    }

    /**
     * Registers the new schema in the registries.
     *
     * @param registries Registries before registering this schema.
     * @param tableId ID of the table to which the schema belongs.
     * @param schema The schema to register.
     * @return Registries after registering this schema.
     */
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

        if (registry != null && schemaVer <= registry.lastKnownSchemaVersion()) {
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
    private CompletableFuture<?> dropRegistry(long causalityToken, int tableId) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            return registriesVv.update(causalityToken, (registries, e) -> inBusyLock(busyLock, () -> {
                if (e != null) {
                    return failedFuture(new IgniteInternalException(
                            format("Cannot remove a schema registry for the table [tblId={}]", tableId), e));
                }

                Map<Integer, SchemaRegistryImpl> regs = new HashMap<>(registries);

                SchemaRegistryImpl removedRegistry = regs.remove(tableId);
                removedRegistry.close();

                return completedFuture(regs);
            }));
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public void stop() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        //noinspection ConstantConditions
        IgniteUtils.closeAllManually(registriesVv.latest().values());
    }
}
