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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Operations.noop;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.CreateTableEventParameters;
import org.apache.ignite.internal.catalog.events.TableEventParameters;
import org.apache.ignite.internal.causality.CompletionListener;
import org.apache.ignite.internal.causality.IncrementalVersionedValue;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.manager.Producer;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.schema.event.SchemaEvent;
import org.apache.ignite.internal.schema.event.SchemaEventParameters;
import org.apache.ignite.internal.schema.marshaller.schema.SchemaSerializerImpl;
import org.apache.ignite.internal.schema.registry.SchemaRegistryImpl;
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
    }

    private CompletableFuture<Boolean> onTableCreated(CatalogEventParameters event, @Nullable Throwable ex) {
        CreateTableEventParameters creationEvent = (CreateTableEventParameters) event;

        return onTableCreatedOrAltered(creationEvent.tableDescriptor(), creationEvent.causalityToken(), ex);
    }

    private CompletableFuture<Boolean> onTableAltered(CatalogEventParameters event, @Nullable Throwable ex) {
        assert event instanceof TableEventParameters;

        TableEventParameters tableEvent = ((TableEventParameters) event);

        CatalogTableDescriptor tableDescriptor = catalogService.table(tableEvent.tableId(), tableEvent.catalogVersion());

        assert tableDescriptor != null;

        return onTableCreatedOrAltered(tableDescriptor, event.causalityToken(), ex);
    }

    private CompletableFuture<Boolean> onTableCreatedOrAltered(
            CatalogTableDescriptor tableDescriptor,
            long causalityToken,
            @Nullable Throwable incomingEx
    ) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            if (incomingEx != null) {
                return failedFuture(incomingEx);
            }

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

                return registerSchema(registries, tableId, newSchema);
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
            prevSchema = schemaByVersion(tableId, prevVersion).get();
        }

        schema.columnMapping(SchemaUtils.columnMapper(prevSchema, schema));
    }

    /**
     * Registers the new schema in a Schema Registry.
     *
     * @param registries Map of schema registries.
     * @param tableId Table id.
     * @param schema Schema descriptor.
     * @return Future that, when complete, will resolve into an updated map of schema registries
     *     (to be used in {@link IncrementalVersionedValue#update}).
     */
    private CompletableFuture<Map<Integer, SchemaRegistryImpl>> registerSchema(
            Map<Integer, SchemaRegistryImpl> registries,
            int tableId,
            SchemaDescriptor schema
    ) {
        ByteArray key = schemaWithVerHistKey(tableId, schema.version());

        byte[] serializedSchema = SchemaSerializerImpl.INSTANCE.serialize(schema);

        return metastorageMgr.invoke(notExists(key), put(key, serializedSchema), noop())
                .thenApply(t -> {
                    SchemaRegistryImpl reg = registries.get(tableId);

                    if (reg == null) {
                        Map<Integer, SchemaRegistryImpl> copy = new HashMap<>(registries);

                        copy.put(tableId, createSchemaRegistry(tableId, schema));

                        return copy;
                    } else {
                        reg.onSchemaRegistered(schema);

                        return registries;
                    }
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
                ver -> inBusyLock(busyLock, () -> tableSchema(tableId, ver)),
                () -> inBusyLock(busyLock, () -> latestSchemaVersion(tableId)),
                initialSchema
        );
    }

    /**
     * Return table schema of certain version from history.
     *
     * @param tblId Table id.
     * @param schemaVer Schema version.
     * @return Schema descriptor.
     */
    private CompletableFuture<SchemaDescriptor> tableSchema(int tblId, int schemaVer) {
        CompletableFuture<SchemaDescriptor> fut = new CompletableFuture<>();

        SchemaRegistry registry = registriesVv.latest().get(tblId);

        if (registry.lastSchemaVersion() > schemaVer) {
            return schemaByVersion(tblId, schemaVer);
        }

        CompletionListener<Map<Integer, SchemaRegistryImpl>> schemaListener = (token, regs, e) -> {
            if (schemaVer <= regs.get(tblId).lastSchemaVersion()) {
                SchemaRegistry registry0 = registriesVv.latest().get(tblId);

                SchemaDescriptor desc = registry0.schemaCached(schemaVer);

                assert desc != null : "Unexpected empty schema description.";

                fut.complete(desc);
            }
        };

        registriesVv.whenComplete(schemaListener);

        // This check is needed for the case when we have registered schemaListener,
        // but registriesVv has already been completed, so listener would be triggered only for the next versioned value update.
        if (checkSchemaVersion(tblId, schemaVer)) {
            registriesVv.removeWhenComplete(schemaListener);

            registry = registriesVv.latest().get(tblId);

            SchemaDescriptor desc = registry.schemaCached(schemaVer);

            assert desc != null : "Unexpected empty schema description.";

            fut.complete(desc);
        }

        return fut.thenApply(res -> {
            registriesVv.removeWhenComplete(schemaListener);
            return res;
        });
    }

    /**
     * Checks that the provided schema version is less or equal than the latest version from the schema registry.
     *
     * @param tblId Unique table id.
     * @param schemaVer Schema version for the table.
     * @return True, if the schema version is less or equal than the latest version from the schema registry, false otherwise.
     */
    private boolean checkSchemaVersion(int tblId, int schemaVer) {
        SchemaRegistry registry = registriesVv.latest().get(tblId);

        assert registry != null : IgniteStringFormatter.format("Registry for the table not found [tblId={}]", tblId);

        return schemaVer <= registry.lastSchemaVersion();
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
     * Drop schema registry for the given table id.
     *
     * @param causalityToken Causality token.
     * @param tableId Table id.
     */
    public CompletableFuture<?> dropRegistry(long causalityToken, int tableId) {
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

    @Override
    public void stop() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();
    }

    /**
     * Gets the latest version of the table schema which is available in Metastore.
     *
     * @param tblId Table id.
     * @return The latest schema version.
     */
    private CompletableFuture<Integer> latestSchemaVersion(int tblId) {
        var latestVersionFuture = new CompletableFuture<Integer>();

        metastorageMgr.prefix(schemaHistPrefix(tblId)).subscribe(new Subscriber<>() {
            private int lastVer = CatalogTableDescriptor.INITIAL_TABLE_VERSION;

            @Override
            public void onSubscribe(Subscription subscription) {
                // Request unlimited demand.
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(Entry item) {
                String key = new String(item.key(), UTF_8);
                int descVer = extractVerFromSchemaKey(key);

                if (descVer > lastVer) {
                    lastVer = descVer;
                }
            }

            @Override
            public void onError(Throwable throwable) {
                latestVersionFuture.completeExceptionally(throwable);
            }

            @Override
            public void onComplete() {
                latestVersionFuture.complete(lastVer);
            }
        });

        return latestVersionFuture;
    }

    /**
     * Gets the defined version of the table schema which available in Metastore.
     *
     * @param tblId Table id.
     * @return Schema representation if schema found, {@code null} otherwise.
     */
    private CompletableFuture<SchemaDescriptor> schemaByVersion(int tblId, int ver) {
        return metastorageMgr.get(schemaWithVerHistKey(tblId, ver))
                .thenApply(entry -> {
                    byte[] value = entry.value();

                    assert value != null;

                    return SchemaSerializerImpl.INSTANCE.deserialize(value);
                });
    }

    private static int extractVerFromSchemaKey(String key) {
        int pos = key.lastIndexOf('.');
        assert pos != -1 : "Unexpected key: " + key;

        key = key.substring(pos + 1);
        return Integer.parseInt(key);
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

    /**
     * Forms schema history predicate.
     *
     * @param tblId Table id.
     * @return {@link ByteArray} representation.
     */
    private static ByteArray schemaHistPrefix(int tblId) {
        return ByteArray.fromString(tblId + SCHEMA_STORE_PREFIX);
    }
}
