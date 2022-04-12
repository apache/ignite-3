/*
 * Copyright 2022 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.internal.table;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.internal.causality.VersionedValue;
import org.apache.ignite.internal.configuration.schema.ExtendedTableConfiguration;
import org.apache.ignite.internal.configuration.schema.SchemaConfiguration;
import org.apache.ignite.internal.configuration.schema.SchemaView;
import org.apache.ignite.internal.manager.EventListener;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.manager.Producer;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.schema.SchemaSerializerImpl;
import org.apache.ignite.internal.schema.registry.SchemaRegistryImpl;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.directProxy;
import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.getByInternalId;

/**
 * The class services a management of table schemas.
 */
public class SchemaManager extends Producer<SchemaEvent, SchemaEventParameters> implements IgniteComponent {
    public static final int INITIAL_SCHEMA_VERSION = 1;

    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(SchemaManager.class);

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Tables configuration. */
    private final TablesConfiguration tablesCfg;

    /** Versioned store for tables by name. */
    private final VersionedValue<Map<UUID, SchemaRegistryImpl>> registriesVv;

    /** Constructor. */
    public SchemaManager(Consumer<Consumer<Long>> registry, TablesConfiguration tablesCfg) {
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
        long causalityToken = schemasCtx.storageRevision();

        ExtendedTableConfiguration tblCfg = schemasCtx.config(ExtendedTableConfiguration.class);

        UUID tblId = tblCfg.id().value();

        String tableName = tblCfg.name().value();

        SchemaDescriptor schemaDescriptor = SchemaSerializerImpl.INSTANCE.deserialize((schemasCtx.newValue().schema()));

        createSchema(causalityToken, tblId, tableName, schemaDescriptor);

        fireEvent(SchemaEvent.CREATE, new SchemaEventParameters(causalityToken, tblId, schemaDescriptor), null);

        return CompletableFuture.completedFuture(null);
    }

    private void createSchema(long causalityToken, UUID tableId, String tableName, SchemaDescriptor schemaDescriptor) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }

        try {
            createSchemaInternal(causalityToken, tableId, tableName, schemaDescriptor);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private void createSchemaInternal(long causalityToken, UUID tableId, String tableName, SchemaDescriptor schemaDescriptor) {
        registriesVv.update(causalityToken, registries -> {
            SchemaRegistryImpl reg = registries.get(tableId);

            if (reg == null) {
                registries = new HashMap<>(registries);

                SchemaRegistryImpl registry = createSchemaRegistry(tableId, tableName, schemaDescriptor);

                registries.put(tableId, registry);
            } else {
                reg.onSchemaRegistered(schemaDescriptor);
            }

            return registries;
        }, th -> {
            throw new IgniteInternalException(IgniteStringFormatter.format("Cannot create a schema for the table "
                + "[tblId={}, ver={}]", tableId, schemaDescriptor.version()), th);
        });
    }

    private SchemaRegistryImpl createSchemaRegistry(UUID tableId, String tableName, SchemaDescriptor initialSchema) {
        return new SchemaRegistryImpl(ver -> {
                if (!busyLock.enterBusy()) {
                    throw new IgniteException(new NodeStoppingException());
                }

                try {
                    return tableSchema(tableId, tableName, ver);
                } finally {
                    busyLock.leaveBusy();
                }
            }, () -> {
                if (!busyLock.enterBusy()) {
                    throw new IgniteException(new NodeStoppingException());
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
            public boolean notify(@NotNull SchemaEventParameters parameters, @Nullable Throwable exception) {
                if (tblId.equals(parameters.tableId()) && schemaVer <= parameters.schemaDescriptor().version()) {
                    fut.complete(getSchemaDescriptorLocally(schemaVer, tblCfg));

                    return true;
                }

                return false;
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

    public CompletableFuture<Map<UUID, SchemaRegistryImpl>> registries(long causalityToken) {
        return registriesVv.get(causalityToken);
    }

    public CompletableFuture<SchemaRegistry> schemaRegistry(long causalityToken, UUID tableId) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }

        try {
            return registriesVv.get(causalityToken).thenApply(regs -> regs.get(tableId));
        } finally {
            busyLock.leaveBusy();
        }
    }

    public void dropRegistry(long causalityToken, UUID tableId) {
        registriesVv.update(causalityToken, registries -> {
            registries = new HashMap<>(registries);

            registries.remove(tableId);

            return registries;
        }, th -> {
            throw new IgniteInternalException(IgniteStringFormatter.format("Cannot remove a schema registry for the table "
                + "[tblId={}]", tableId), th);
        });
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();
    }
}
