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
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Operations.noop;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.causality.IncrementalVersionedValue;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.manager.Producer;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.schema.configuration.ColumnView;
import org.apache.ignite.internal.schema.configuration.ExtendedTableView;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.event.SchemaEvent;
import org.apache.ignite.internal.schema.event.SchemaEventParameters;
import org.apache.ignite.internal.schema.marshaller.schema.SchemaSerializerImpl;
import org.apache.ignite.internal.schema.registry.SchemaRegistryImpl;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.Nullable;

/**
 * The class services a management of table schemas.
 */
public class SchemaManager extends Producer<SchemaEvent, SchemaEventParameters> implements IgniteComponent {
    private static final IgniteLogger LOGGER = Loggers.forClass(SchemaManager.class);

    /** Initial version for schemas. */
    public static final int INITIAL_SCHEMA_VERSION = 1;

    /** Schema history key predicate part. */
    private static final String SCHEMA_STORE_PREFIX = ".sch-hist.";

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Tables configuration. */
    private final TablesConfiguration tablesCfg;

    /** Versioned store for tables by ID. */
    private final IncrementalVersionedValue<Map<Integer, SchemaRegistryImpl>> registriesVv;

    /** Meta storage manager. */
    private final MetaStorageManager metastorageMgr;

    /** Constructor. */
    public SchemaManager(
            Consumer<LongFunction<CompletableFuture<?>>> registry,
            TablesConfiguration tablesCfg,
            MetaStorageManager metastorageMgr
    ) {
        this.registriesVv = new IncrementalVersionedValue<>(registry, HashMap::new);
        this.tablesCfg = tablesCfg;
        this.metastorageMgr = metastorageMgr;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        tablesCfg.tables().any().columns().listen(this::onSchemaChange);
    }

    /**
     * Listener of schema configuration changes.
     *
     * @param ctx Configuration context.
     * @return A future.
     */
    private CompletableFuture<?> onSchemaChange(ConfigurationNotificationEvent<NamedListView<ColumnView>> ctx) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException()));
        }

        try {
            ExtendedTableView tblCfg = ctx.newValue(ExtendedTableView.class);

            int newSchemaVersion = tblCfg.schemaId();

            int tblId = tblCfg.id();

            if (searchSchemaByVersion(tblId, newSchemaVersion) != null) {
                return completedFuture(null);
            }

            SchemaDescriptor newSchema = SchemaUtils.prepareSchemaDescriptor(newSchemaVersion, tblCfg);

            // This is intentionally a blocking call to enforce configuration listener execution order. Unfortunately it is not possible
            // to execute this method asynchronously, because the schema descriptor is needed to fire the CREATE event as a synchronous part
            // of the configuration listener.
            try {
                setColumnMapping(newSchema, tblId);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                return failedFuture(e);
            } catch (ExecutionException e) {
                return failedFuture(e);
            }

            long causalityToken = ctx.storageRevision();

            // Fire event early, because dependent listeners have to register VersionedValues' update futures
            var eventParams = new SchemaEventParameters(causalityToken, tblId, newSchema);

            fireEvent(SchemaEvent.CREATE, eventParams)
                    .whenComplete((v, e) -> {
                        if (e != null) {
                            LOGGER.warn("Error when processing CREATE event", e);
                        }
                    });

            return registriesVv.update(causalityToken, (registries, e) -> inBusyLock(busyLock, () -> {
                if (e != null) {
                    return failedFuture(new IgniteInternalException(IgniteStringFormatter.format(
                            "Cannot create a schema for the table [tblId={}, ver={}]", tblId, newSchemaVersion), e)
                    );
                }

                return saveSchemaDescriptor(tblId, newSchema)
                        .thenApply(t -> registerSchema(tblId, newSchema, registries));
            }));
        } finally {
            busyLock.leaveBusy();
        }
    }

    private Map<Integer, SchemaRegistryImpl> registerSchema(int tblId, SchemaDescriptor newSchema,
            Map<Integer, SchemaRegistryImpl> registries) {
        SchemaRegistryImpl reg = registries.get(tblId);

        if (reg == null) {
            Map<Integer, SchemaRegistryImpl> copy = new HashMap<>(registries);

            copy.put(tblId, createSchemaRegistry(tblId, newSchema));

            return copy;
        } else {
            reg.onSchemaRegistered(newSchema);

            return registries;
        }
    }

    private void setColumnMapping(SchemaDescriptor schema, int tableId) throws ExecutionException, InterruptedException {
        if (schema.version() == INITIAL_SCHEMA_VERSION) {
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
     * Gets a schema descriptor from the configuration storage.
     *
     * @param tableId Table ID.
     * @param schemaVer Schema version.
     * @return Schema descriptor.
     */
    private CompletableFuture<SchemaDescriptor> loadSchemaDescriptor(int tableId, int schemaVer) {
        CompletableFuture<Entry> ent = metastorageMgr.get(schemaWithVerHistKey(tableId, schemaVer));

        return ent.thenApply(e -> SchemaSerializerImpl.INSTANCE.deserialize(e.value()));
    }

    /** Saves a schema descriptor to the configuration storage. */
    private CompletableFuture<Boolean> saveSchemaDescriptor(int tableId, SchemaDescriptor schema) {
        ByteArray key = schemaWithVerHistKey(tableId, schema.version());

        byte[] serializedSchema = SchemaSerializerImpl.INSTANCE.serialize(schema);

        return metastorageMgr.invoke(notExists(key), put(key, serializedSchema), noop());
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
                () -> inBusyLock(busyLock, () -> latestSchemaVersion(tableId)),
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

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        IgniteUtils.closeAllManually(registriesVv.latest().values().stream());
    }

    /**
     * Gets the latest version of the table schema which available in Metastore.
     *
     * @param tblId Table id.
     * @return The latest schema version.
     */
    private CompletableFuture<Integer> latestSchemaVersion(int tblId) {
        var latestVersionFuture = new CompletableFuture<Integer>();

        metastorageMgr.prefix(schemaHistPrefix(tblId)).subscribe(new Subscriber<>() {
            private int lastVer = INITIAL_SCHEMA_VERSION;

            @Override
            public void onSubscribe(Subscription subscription) {
                // Request unlimited demand.
                subscription.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(Entry item) {
                String key = new String(item.key(), StandardCharsets.UTF_8);
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

    private int extractVerFromSchemaKey(String key) {
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
