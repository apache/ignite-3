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

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.closeAllManually;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableSchemaVersions;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.CreateTableEventParameters;
import org.apache.ignite.internal.catalog.events.TableEventParameters;
import org.apache.ignite.internal.causality.IncrementalVersionedValue;
import org.apache.ignite.internal.causality.RevisionListenerRegistry;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.schema.catalog.CatalogToSchemaDescriptorConverter;
import org.apache.ignite.internal.schema.registry.SchemaRegistryImpl;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
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

    /** Versioned value for linearizing index partition changing events. */
    private final IncrementalVersionedValue<Void> registriesVv;

    /** Schema registries by table ID. */
    private final Map<Integer, SchemaRegistryImpl> registriesById = new ConcurrentHashMap<>();

    /** Constructor. */
    public SchemaManager(RevisionListenerRegistry registry, CatalogService catalogService) {
        this.registriesVv = new IncrementalVersionedValue<>("SchemaManager#registries", registry);
        this.catalogService = catalogService;
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        catalogService.listen(CatalogEvent.TABLE_CREATE, this::onTableCreated);
        catalogService.listen(CatalogEvent.TABLE_ALTER, this::onTableAltered);

        registerExistingTables();

        return nullCompletedFuture();
    }

    private void registerExistingTables() {
        for (int catalogVer = catalogService.latestCatalogVersion(); catalogVer >= catalogService.earliestCatalogVersion(); catalogVer--) {
            Collection<CatalogTableDescriptor> tables = catalogService.catalog(catalogVer).tables();

            for (CatalogTableDescriptor tableDescriptor : tables) {
                int tableId = tableDescriptor.id();

                if (registriesById.containsKey(tableId)) {
                    continue;
                }

                registerVersions(tableDescriptor, null);
            }
        }
    }

    private void registerVersions(
            CatalogTableDescriptor tableDescriptor, @Nullable SchemaDescriptor lastKnownSchema
    ) {
        SchemaDescriptor prevSchema = lastKnownSchema;
        CatalogTableSchemaVersions schemaVersions = tableDescriptor.schemaVersions();
        int versionToStart = lastKnownSchema != null 
                ? lastKnownSchema.version() + 1 
                : schemaVersions.earliestVersion();
        for (int tableVer = versionToStart; tableVer <= schemaVersions.latestVersion(); tableVer++) {
            SchemaDescriptor newSchema = CatalogToSchemaDescriptorConverter.convert(tableDescriptor, tableVer);

            if (prevSchema != null) {
                newSchema.columnMapping(SchemaUtils.columnMapper(prevSchema, newSchema));
            }

            prevSchema = newSchema;

            registerSchema(tableDescriptor.id(), newSchema);
        }
    }

    private CompletableFuture<Boolean> onTableCreated(CatalogEventParameters event) {
        CreateTableEventParameters creationEvent = (CreateTableEventParameters) event;

        return onTableCreatedOrAltered(creationEvent.tableDescriptor(), creationEvent.causalityToken());
    }

    private CompletableFuture<Boolean> onTableAltered(CatalogEventParameters event) {
        assert event instanceof TableEventParameters;

        TableEventParameters tableEvent = ((TableEventParameters) event);

        CatalogTableDescriptor tableDescriptor = catalogService.catalog(tableEvent.catalogVersion()).table(tableEvent.tableId());

        assert tableDescriptor != null;

        return onTableCreatedOrAltered(tableDescriptor, event.causalityToken());
    }

    private CompletableFuture<Boolean> onTableCreatedOrAltered(CatalogTableDescriptor tableDescriptor, long causalityToken) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            int tableId = tableDescriptor.id();
            int newSchemaVersion = tableDescriptor.latestSchemaVersion();

            if (searchSchemaByVersion(tableId, newSchemaVersion) != null) {
                return falseCompletedFuture();
            }

            registerVersions(tableDescriptor, lastKnownSchemaVersion(tableId));

            return registriesVv.update(causalityToken, (registries, e) -> inBusyLock(busyLock, () -> {
                if (e != null) {
                    return failedFuture(new IgniteInternalException(format(
                            "Cannot create a schema for the table [tblId={}, ver={}]", tableId, newSchemaVersion), e)
                    );
                }

                return nullCompletedFuture();
            })).thenApply(ignored -> false);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Loads the table schema descriptor by version from local Catalog.
     * If called with a schema version for which the schema is not yet saved to the Catalog, {@code null} if returned.
     *
     * @param tblId Table id.
     * @param ver Schema version (must not be higher than the latest version saved to the Catalog).
     * @return Schema representation (or {@code null}).
     */
    private @Nullable SchemaDescriptor loadOptionalSchemaDescriptor(int tblId, int ver) {
        int catalogVersion = catalogService.latestCatalogVersion();

        while (catalogVersion >= catalogService.earliestCatalogVersion()) {
            CatalogTableDescriptor tableDescriptor = catalogService.catalog(catalogVersion).table(tblId);

            if (tableDescriptor == null) {
                catalogVersion--;

                continue;
            }

            return CatalogToSchemaDescriptorConverter.convertIfExists(tableDescriptor, ver);
        }

        throw new AssertionError(format("Schema descriptor is not found [tableId={}, schemaId={}]", tblId, ver));
    }

    /**
     * Registers the new schema in the registries.
     *
     * @param tableId ID of the table to which the schema belongs.
     * @param schema The schema to register.
     */
    private void registerSchema(
            int tableId,
            SchemaDescriptor schema
    ) {
        registriesById.compute(tableId, (tableId0, reg) -> {
            if (reg == null) {
                return createSchemaRegistry(tableId0, schema);
            }

            reg.onSchemaRegistered(schema);

            return reg;
        });
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
                ver -> inBusyLock(busyLock, () -> loadOptionalSchemaDescriptor(tableId, ver)),
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
        SchemaRegistry registry = registriesById.get(tblId);

        if (registry != null && schemaVer <= registry.lastKnownSchemaVersion()) {
            return registry.schema(schemaVer);
        } else {
            return null;
        }
    }

    /** Returns the last cached version of schema for a given table, or {@code null} if registry for the table has not been registered. */
    private @Nullable SchemaDescriptor lastKnownSchemaVersion(int tblId) {
        SchemaRegistry registry = registriesById.get(tblId);

        if (registry != null) {
            return registry.lastKnownSchema();
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
                    .thenApply(unused -> inBusyLock(busyLock, () -> registriesById.get(tableId)));
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
        return registriesById.get(tableId);
    }

    /**
     * Drops schema registry for the given table id (along with the corresponding schemas).
     *
     * @param tableId Table id.
     */
    public void dropRegistry(int tableId) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            SchemaRegistryImpl removedRegistry = registriesById.remove(tableId);

            removedRegistry.close();
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        if (!stopGuard.compareAndSet(false, true)) {
            return nullCompletedFuture();
        }

        busyLock.block();

        //noinspection ConstantConditions
        try {
            closeAllManually(registriesById.values());
        } catch (Exception e) {
            return failedFuture(e);
        }

        return nullCompletedFuture();
    }
}
