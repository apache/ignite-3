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
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.causality.VersionedValue;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.manager.Producer;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.dsl.Conditions;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.apache.ignite.internal.schema.configuration.ColumnView;
import org.apache.ignite.internal.schema.configuration.ExtendedTableConfiguration;
import org.apache.ignite.internal.schema.configuration.ExtendedTableView;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.event.SchemaEvent;
import org.apache.ignite.internal.schema.event.SchemaEventParameters;
import org.apache.ignite.internal.schema.marshaller.schema.SchemaSerializerImpl;
import org.apache.ignite.internal.schema.registry.SchemaRegistryImpl;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.IgniteTriConsumer;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.Nullable;

/**
 * The class services a management of table schemas.
 */
public class SchemaManager extends Producer<SchemaEvent, SchemaEventParameters> implements IgniteComponent {
    /** Initial version for schemas. */
    public static final int INITIAL_SCHEMA_VERSION = 1;

    /** Schema history key predicate part. */
    public static final String SCHEMA_STORE_PREFIX = ".sch-hist.";

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Tables configuration. */
    private final TablesConfiguration tablesCfg;

    /** Versioned store for tables by name. */
    private final VersionedValue<Map<UUID, SchemaRegistryImpl>> registriesVv;

    /** Meta storage manager. */
    private final MetaStorageManager metastorageMgr;

    /** Constructor. */
    public SchemaManager(
            Consumer<Function<Long, CompletableFuture<?>>> registry,
            TablesConfiguration tablesCfg,
            MetaStorageManager metastorageMgr
    ) {
        this.registriesVv = new VersionedValue<>(registry, HashMap::new);
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
            ExtendedTableView tblCfg = (ExtendedTableView) ctx.config(ExtendedTableConfiguration.class).value();

            int verFromUpdate = tblCfg.schemaId();

            UUID tblId = tblCfg.id();

            String tableName = tblCfg.name();

            SchemaDescriptor schemaDescFromUpdate = SchemaUtils.prepareSchemaDescriptor(verFromUpdate, tblCfg);

            if (searchSchemaByVersion(tblId, schemaDescFromUpdate.version()) != null) {
                return completedFuture(null);
            }

            if (verFromUpdate != INITIAL_SCHEMA_VERSION) {
                SchemaDescriptor oldSchema = searchSchemaByVersion(tblId, verFromUpdate - 1);

                if (oldSchema == null) {
                    byte[] serPrevSchema = schemaByVersion(tblId, verFromUpdate - 1);

                    assert serPrevSchema != null;

                    oldSchema = SchemaSerializerImpl.INSTANCE.deserialize(serPrevSchema);
                }

                schemaDescFromUpdate.columnMapping(SchemaUtils.columnMapper(oldSchema, schemaDescFromUpdate));
            }

            long causalityToken = ctx.storageRevision();

            CompletableFuture<?> createSchemaFut = createSchema(causalityToken, tblId, tableName, schemaDescFromUpdate);

            try {
                final ByteArray key = schemaWithVerHistKey(tblId, verFromUpdate);

                createSchemaFut.thenCompose(t -> metastorageMgr.invoke(
                        Conditions.notExists(key),
                        Operations.put(key, SchemaSerializerImpl.INSTANCE.serialize(schemaDescFromUpdate)),
                        Operations.noop()));
            } catch (Throwable th) {
                createSchemaFut.completeExceptionally(th);
            }

            createSchemaFut.whenComplete((ignore, th) -> {
                if (th == null) {
                    registriesVv.get(causalityToken).thenRun(() -> inBusyLock(busyLock,
                            () -> fireEvent(SchemaEvent.CREATE, new SchemaEventParameters(causalityToken, tblId, schemaDescFromUpdate))));
                }
            });

            return createSchemaFut;
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Create new schema locally.
     *
     * @param causalityToken Causality token.
     * @param tableId Table id.
     * @param tableName Table name.
     * @param schemaDescriptor Schema descriptor.
     * @return Create schema future.
     */
    private CompletableFuture<?> createSchema(long causalityToken, UUID tableId, String tableName, SchemaDescriptor schemaDescriptor) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }

        try {
            return createSchemaInternal(causalityToken, tableId, tableName, schemaDescriptor);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Internal method for creating schema locally.
     *
     * @param causalityToken Causality token.
     * @param tableId Table id.
     * @param tableName Table name.
     * @param schemaDescriptor Schema descriptor.
     * @return Create schema future.
     */
    private CompletableFuture<?> createSchemaInternal(
            long causalityToken,
            UUID tableId,
            String tableName,
            SchemaDescriptor schemaDescriptor
    ) {
        return registriesVv.update(causalityToken, (registries, e) -> inBusyLock(busyLock, () -> {
            if (e != null) {
                return failedFuture(new IgniteInternalException(IgniteStringFormatter.format(
                        "Cannot create a schema for the table [tblId={}, ver={}]", tableId, schemaDescriptor.version()), e)
                );
            }

            Map<UUID, SchemaRegistryImpl> regs = registries;

            SchemaRegistryImpl reg = regs.get(tableId);

            if (reg == null) {
                regs = new HashMap<>(registries);

                SchemaRegistryImpl registry = createSchemaRegistry(tableId, tableName, schemaDescriptor);

                regs.put(tableId, registry);
            } else {
                reg.onSchemaRegistered(schemaDescriptor);
            }

            return completedFuture(regs);
        }));
    }

    /**
     * Create schema registry for the table.
     *
     * @param tableId Table id.
     * @param tableName Table name.
     * @param initialSchema Initial schema for the registry.
     * @return Schema registry.
     */
    private SchemaRegistryImpl createSchemaRegistry(UUID tableId, String tableName, SchemaDescriptor initialSchema) {
        return new SchemaRegistryImpl(ver -> {
            if (!busyLock.enterBusy()) {
                throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
            }

            try {
                return tableSchema(tableId, tableName, ver);
            } finally {
                busyLock.leaveBusy();
            }
        }, () -> {
            if (!busyLock.enterBusy()) {
                throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
            }

            try {
                return latestSchemaVersion(tableId);
            } finally {
                busyLock.leaveBusy();
            }
        },
            initialSchema
        );
    }

    /**
     * Return table schema of certain version from history.
     *
     * @param tblId     Table id.
     * @param schemaVer Schema version.
     * @return Schema descriptor.
     */
    private CompletableFuture<SchemaDescriptor> tableSchema(UUID tblId, String tableName, int schemaVer) {
        ExtendedTableConfiguration tblCfg = ((ExtendedTableConfiguration) tablesCfg.tables().get(tableName));

        CompletableFuture<SchemaDescriptor> fut = new CompletableFuture<>();

        SchemaRegistry registry = registriesVv.latest().get(tblId);

        if (registry.lastSchemaVersion() > schemaVer) {
            return getSchemaDescriptor(schemaVer, tblCfg);
        }

        IgniteTriConsumer<Long, Map<UUID, SchemaRegistryImpl>, Throwable> schemaListener = (token, regs, e) -> {
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
    private boolean checkSchemaVersion(UUID tblId, int schemaVer) {
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
    private SchemaDescriptor searchSchemaByVersion(UUID tblId, int schemaVer) {
        SchemaRegistry registry = registriesVv.latest().get(tblId);

        if (registry != null && schemaVer <= registry.lastSchemaVersion()) {
            return registry.schema(schemaVer);
        } else {
            return null;
        }
    }

    /**
     * Gets a schema descriptor from the configuration storage.
     *
     * @param schemaVer Schema version.
     * @param tblCfg    Table configuration.
     * @return Schema descriptor.
     */
    private CompletableFuture<SchemaDescriptor> getSchemaDescriptor(int schemaVer, ExtendedTableConfiguration tblCfg) {
        CompletableFuture<Entry> ent = metastorageMgr.get(
                schemaWithVerHistKey(tblCfg.id().value(), schemaVer));

        return ent.thenApply(e -> SchemaSerializerImpl.INSTANCE.deserialize(e.value()));
    }

    /**
     * Get the schema registry for the given causality token and table id.
     *
     * @param causalityToken Causality token.
     * @param tableId Id of a table which the required registry belongs to. If {@code null}, then this method will return
     *                a future which will be completed with {@code null} result, but only when the schema manager will have
     *                consistent state regarding given causality token.
     * @return A future which will be completed when schema registries for given causality token are ready.
     */
    public CompletableFuture<SchemaRegistry> schemaRegistry(long causalityToken, @Nullable UUID tableId) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            return registriesVv.get(causalityToken)
                    .thenApply(regs -> inBusyLock(busyLock, () -> tableId == null ? null : regs.get(tableId)));
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
    public SchemaRegistry schemaRegistry(UUID tableId) {
        return registriesVv.latest().get(tableId);
    }

    /**
     * Drop schema registry for the given table id.
     *
     * @param causalityToken Causality token.
     * @param tableId Table id.
     */
    public CompletableFuture<?> dropRegistry(long causalityToken, UUID tableId) {
        return registriesVv.update(causalityToken, (registries, e) -> inBusyLock(busyLock, () -> {
            if (e != null) {
                return failedFuture(new IgniteInternalException(
                        IgniteStringFormatter.format("Cannot remove a schema registry for the table [tblId={}]", tableId), e));
            }

            Map<UUID, SchemaRegistryImpl> regs = new HashMap<>(registries);

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
    }

    /**
     * Gets the latest version of the table schema which available in Metastore.
     *
     * @param tblId Table id.
     * @return The latest schema version.
     */
    private int latestSchemaVersion(UUID tblId) {
        try (Cursor<Entry> cur = metastorageMgr.prefix(schemaHistPrefix(tblId))) {
            int lastVer = INITIAL_SCHEMA_VERSION;

            for (Entry ent : cur) {
                String key = ent.key().toString();
                int descVer = extractVerFromSchemaKey(key);

                if (descVer > lastVer) {
                    lastVer = descVer;
                }
            }

            return lastVer;
        } catch (NodeStoppingException e) {
            throw new IgniteException(e.traceId(), e.code(), e.getMessage(), e);
        }
    }

    /**
     * Gets the defined version of the table schema which available in Metastore.
     *
     * @param tblId Table id.
     * @return Schema representation if schema found, {@code null} otherwise.
     */
    private byte[] schemaByVersion(UUID tblId, int ver) {
        try {
            Cursor<Entry> cur = metastorageMgr.prefix(schemaWithVerHistKey(tblId, ver));

            if (cur.hasNext()) {
                Entry ent = cur.next();

                assert !cur.hasNext();

                return ent.value();
            }

            return null;
        } catch (NodeStoppingException e) {
            throw new IgniteException(e.traceId(), e.code(), e.getMessage(), e);
        }
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
    private static ByteArray schemaWithVerHistKey(UUID tblId, int ver) {
        return ByteArray.fromString(tblId + SCHEMA_STORE_PREFIX + ver);
    }

    /**
     * Forms schema history predicate.
     *
     * @param tblId Table id.
     * @return {@link ByteArray} representation.
     */
    private static ByteArray schemaHistPrefix(UUID tblId) {
        return ByteArray.fromString(tblId + SCHEMA_STORE_PREFIX);
    }
}
