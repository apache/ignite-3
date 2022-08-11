/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.getByInternalId;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.causality.VersionedValue;
import org.apache.ignite.internal.configuration.schema.ExtendedTableConfiguration;
import org.apache.ignite.internal.configuration.schema.SchemaConfiguration;
import org.apache.ignite.internal.configuration.schema.SchemaView;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.apache.ignite.internal.manager.EventListener;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.manager.Producer;
import org.apache.ignite.internal.schema.event.SchemaEvent;
import org.apache.ignite.internal.schema.event.SchemaEventParameters;
import org.apache.ignite.internal.schema.marshaller.schema.SchemaSerializerImpl;
import org.apache.ignite.internal.schema.registry.SchemaRegistryImpl;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.IgniteSystemProperties;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The class services a management of table schemas.
 */
public class SchemaManager extends Producer<SchemaEvent, SchemaEventParameters> implements IgniteComponent {
    /** Initial version for schemas. */
    public static final int INITIAL_SCHEMA_VERSION = 1;

    /**
     * If this property is set to {@code true} then an attempt to get the configuration property directly from the meta storage will be
     * skipped, and the local property will be returned.
     * TODO: IGNITE-16774 This property and overall approach, access configuration directly through the Metostorage,
     * TODO: will be removed after fix of the issue.
     */
    private final boolean getMetadataLocallyOnly = IgniteSystemProperties.getBoolean("IGNITE_GET_METADATA_LOCALLY_ONLY");

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Tables configuration. */
    private final TablesConfiguration tablesCfg;

    /** Versioned store for tables by name. */
    private final VersionedValue<Map<UUID, SchemaRegistryImpl>> registriesVv;

    /** Constructor. */
    public SchemaManager(Consumer<Function<Long, CompletableFuture<?>>> registry, TablesConfiguration tablesCfg) {
        this.registriesVv = new VersionedValue<>(registry, HashMap::new);

        this.tablesCfg = tablesCfg;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        ((ExtendedTableConfiguration) tablesCfg.tables().any()).schemas().listenElements(new ConfigurationNamedListListener<>() {
            @Override
            public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<SchemaView> schemasCtx) {
                return onSchemaCreate(schemasCtx);
            }
        });
    }

    /**
     * Listener of schema configuration changes.
     *
     * @param schemasCtx Schemas configuration context.
     * @return A future.
     */
    private CompletableFuture<?> onSchemaCreate(ConfigurationNotificationEvent<SchemaView> schemasCtx) {
        if (!busyLock.enterBusy()) {
            return failedFuture(new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException()));
        }

        try {
            long causalityToken = schemasCtx.storageRevision();

            ExtendedTableConfiguration tblCfg = schemasCtx.config(ExtendedTableConfiguration.class);

            UUID tblId = tblCfg.id().value();

            String tableName = tblCfg.name().value();

            SchemaDescriptor schemaDescriptor = SchemaSerializerImpl.INSTANCE.deserialize((schemasCtx.newValue().schema()));

            CompletableFuture<?> createSchemaFut = createSchema(causalityToken, tblId, tableName, schemaDescriptor);

            registriesVv.get(causalityToken).thenRun(() -> inBusyLock(busyLock,
                    () -> fireEvent(SchemaEvent.CREATE, new SchemaEventParameters(causalityToken, tblId, schemaDescriptor))));

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
    private SchemaDescriptor tableSchema(UUID tblId, String tableName, int schemaVer) {
        SchemaRegistry registry = registriesVv.latest().get(tblId);

        assert registry != null : IgniteStringFormatter.format("Registry for the table not found [tblId={}]", tblId);

        ExtendedTableConfiguration tblCfg = ((ExtendedTableConfiguration) tablesCfg.tables().get(tableName));

        if (schemaVer <= registry.lastSchemaVersion()) {
            return getSchemaDescriptorLocally(schemaVer, tblCfg);
        }

        CompletableFuture<SchemaDescriptor> fut = new CompletableFuture<>();

        var clo = new EventListener<SchemaEventParameters>() {
            @Override
            public CompletableFuture<Boolean> notify(@NotNull SchemaEventParameters parameters, @Nullable Throwable exception) {
                if (tblId.equals(parameters.tableId()) && schemaVer <= parameters.schemaDescriptor().version()) {
                    fut.complete(getSchemaDescriptorLocally(schemaVer, tblCfg));

                    return completedFuture(true);
                }

                return completedFuture(false);
            }

            @Override
            public void remove(@NotNull Throwable exception) {
                fut.completeExceptionally(exception);
            }
        };

        listen(SchemaEvent.CREATE, clo);

        if (schemaVer <= registry.lastSchemaVersion()) {
            fut.complete(getSchemaDescriptorLocally(schemaVer, tblCfg));
        }

        if (!isSchemaExists(tblId, schemaVer) && fut.complete(null)) {
            removeListener(SchemaEvent.CREATE, clo);
        }

        return fut.join();
    }

    /**
     * Checks that the schema is configured in the Metasorage consensus.
     *
     * @param tblId Table id.
     * @param schemaVer Schema version.
     * @return True when the schema configured, false otherwise.
     */
    private boolean isSchemaExists(UUID tblId, int schemaVer) {
        return latestSchemaVersion(tblId) >= schemaVer;
    }

    /**
     * Gets the latest version of the table schema which available in Metastore.
     *
     * @param tblId Table id.
     * @return The latest schema version.
     */
    private int latestSchemaVersion(UUID tblId) {
        try {
            NamedListView<SchemaView> tblSchemas = ((ExtendedTableConfiguration) getByInternalId(directProxy(tablesCfg.tables()), tblId))
                    .schemas().value();

            int lastVer = INITIAL_SCHEMA_VERSION;

            for (String schemaVerAsStr : tblSchemas.namedListKeys()) {
                int ver = Integer.parseInt(schemaVerAsStr);

                if (ver > lastVer) {
                    lastVer = ver;
                }
            }

            return lastVer;
        } catch (NoSuchElementException e) {
            assert false : "Table must exist. [tableId=" + tblId + ']';

            return INITIAL_SCHEMA_VERSION;
        }
    }

    /**
     * Gets a schema descriptor from the local node configuration storage.
     *
     * @param schemaVer Schema version.
     * @param tblCfg    Table configuration.
     * @return Schema descriptor.
     */
    @NotNull
    private SchemaDescriptor getSchemaDescriptorLocally(int schemaVer, ExtendedTableConfiguration tblCfg) {
        SchemaConfiguration schemaCfg = tblCfg.schemas().get(String.valueOf(schemaVer));

        assert schemaCfg != null;

        return SchemaSerializerImpl.INSTANCE.deserialize(schemaCfg.schema().value());
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
     * Gets a direct accessor for the configuration distributed property.
     * If the metadata access only locally configured the method will return local property accessor.
     *
     * @param property Distributed configuration property to receive direct access.
     * @param <T> Type of the property accessor.
     * @return An accessor for distributive property.
     * @see #getMetadataLocallyOnly
     */
    private <T extends ConfigurationProperty<?>> T directProxy(T property) {
        return getMetadataLocallyOnly ? property : ConfigurationUtil.directProxy(property);
    }
}
