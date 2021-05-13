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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.configuration.internal.ConfigurationManager;
import org.apache.ignite.configuration.schemas.table.ColumnTypeView;
import org.apache.ignite.configuration.schemas.table.ColumnView;
import org.apache.ignite.configuration.schemas.table.TableConfiguration;
import org.apache.ignite.configuration.schemas.table.TableIndexConfiguration;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.configuration.tree.NamedListView;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.schema.registry.SchemaRegistryException;
import org.apache.ignite.internal.schema.registry.SchemaRegistryImpl;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.metastorage.common.Conditions;
import org.apache.ignite.metastorage.common.Cursor;
import org.apache.ignite.metastorage.common.Entry;
import org.apache.ignite.metastorage.common.Key;
import org.apache.ignite.metastorage.common.Operations;
import org.apache.ignite.metastorage.common.WatchEvent;
import org.apache.ignite.metastorage.common.WatchListener;
import org.apache.ignite.schema.PrimaryIndex;
import org.jetbrains.annotations.NotNull;

/**
 * Schema Manager.
 */
public class SchemaManager {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(SchemaManager.class);

    /** Internal prefix for the metasorage. */
    private static final String INTERNAL_PREFIX = "internal.tables.schema.";

    /** Schema history item key suffix. */
    protected static final String INTERNAL_VER_SUFFIX = ".ver.";

    /** Configuration manager in order to handle and listen schema specific configuration. */
    private final ConfigurationManager configurationMgr;

    /** Metastorage manager. */
    private final MetaStorageManager metaStorageMgr;

    /** Vault manager. */
    private final VaultManager vaultManager;

    /** Schema history subscription future. */
    private CompletableFuture<Long> schemaHistorySubscriptionFut;

    /** Schema. */
    private final Map<UUID, SchemaRegistryImpl> schemes = new ConcurrentHashMap<>();

    /**
     * The constructor.
     *
     * @param configurationMgr Configuration manager.
     * @param metaStorageMgr Metastorage manager.
     * @param vaultManager Vault manager.
     */
    public SchemaManager(
        ConfigurationManager configurationMgr,
        MetaStorageManager metaStorageMgr,
        VaultManager vaultManager
    ) {
        this.configurationMgr = configurationMgr;
        this.metaStorageMgr = metaStorageMgr;
        this.vaultManager = vaultManager;

        schemaHistorySubscriptionFut = metaStorageMgr.registerWatchByPrefix(new Key(INTERNAL_PREFIX), new WatchListener() {
            /** {@inheritDoc} */
            @Override public boolean onUpdate(@NotNull Iterable<WatchEvent> events) {
                for (WatchEvent evt : events) {
                    String keyTail = evt.newEntry().key().toString().substring(INTERNAL_PREFIX.length() - 1);

                    int verPos = keyTail.indexOf(INTERNAL_VER_SUFFIX);

                    // Last table schema version changed.
                    if (verPos == -1) {
                        UUID tblId = UUID.fromString(keyTail);

                        if (evt.oldEntry() == null)  // Initial schema added.
                            schemes.put(tblId, new SchemaRegistryImpl());
                        else if (evt.newEntry() == null) // Table Dropped.
                            schemes.remove(tblId);
                        else //TODO: https://issues.apache.org/jira/browse/IGNITE-13752
                            throw new SchemaRegistryException("Schema upgrade is not implemented yet.");
                    }
                    else {
                        UUID tblId = UUID.fromString(keyTail.substring(0, verPos));

                        final SchemaRegistryImpl reg = schemes.get(tblId);

                        assert reg != null : "Table schema was not initialized or table has been dropped: " + tblId;

                        reg.registerSchema((SchemaDescriptor)ByteUtils.fromBytes(evt.newEntry().value()));
                    }
                }

                return true;
            }

            /** {@inheritDoc} */
            @Override public void onError(@NotNull Throwable e) {
                LOG.error("Metastorage listener issue", e);
            }
        });
    }

    /**
     * Unsubscribes a listener form the affinity calculation.
     */
    private void unsubscribeFromAssignmentCalculation() {
        if (schemaHistorySubscriptionFut == null)
            return;

        try {
            Long subscriptionId = schemaHistorySubscriptionFut.get();

            metaStorageMgr.unregisterWatch(subscriptionId);

            schemaHistorySubscriptionFut = null;
        }
        catch (InterruptedException | ExecutionException e) {
            LOG.error("Couldn't unsubscribe for Metastorage updates", e);
        }
    }

    /**
     * Reads current schema configuration, build schema descriptor,
     * then add it to history rise up table schema version.
     *
     * @param tblId Table id.
     * @param tblName Table name.
     * @return Operation future.
     */
    public CompletableFuture<Boolean> initNewSchemaForTable(UUID tblId, String tblName) {
        return vaultManager.get(ByteArray.fromString(INTERNAL_PREFIX + tblId)).
            thenCompose(entry -> {
                TableConfiguration tblConfig = configurationMgr.configurationRegistry().getConfiguration(TablesConfiguration.KEY).tables().get(tblName);
                var key = new Key(INTERNAL_PREFIX + tblId);

                int ver = entry.empty() ? 1 : (int)ByteUtils.bytesToLong(entry.value(), 0) + 1;

                final SchemaDescriptor desc = createSchemaDescriptor(tblConfig, ver);

                return metaStorageMgr.invoke(
                    Conditions.key(key).value().eq(entry.value()), // Won't to rewrite if the version goes ahead.
                    List.of(
                        Operations.put(key, ByteUtils.longToBytes(ver)),
                        Operations.put(new Key(INTERNAL_PREFIX + tblId + INTERNAL_VER_SUFFIX + ver), ByteUtils.toBytes(desc))
                    ),
                    List.of(
                        Operations.noop(),
                        Operations.noop()
                    ));
            });
    }

    /**
     * Creates schema descriptor from config.
     *
     * @param tblConfig Table config.
     * @param ver Schema version.
     * @return Schema descriptor.
     */
    private SchemaDescriptor createSchemaDescriptor(TableConfiguration tblConfig, int ver) {
        final TableIndexConfiguration pkCfg = tblConfig.indices().get(PrimaryIndex.PRIMARY_KEY_INDEX_NAME);

        assert pkCfg != null;

        final Set<String> keyColNames = Stream.of(pkCfg.colNames().value()).collect(Collectors.toSet());
        final NamedListView<ColumnView> cols = tblConfig.columns().value();

        final ArrayList<Column> keyCols = new ArrayList<>(keyColNames.size());
        final ArrayList<Column> valCols = new ArrayList<>(cols.size() - keyColNames.size());

        cols.namedListKeys().stream()
            .map(cols::get)
            .map(col -> new Column(col.name(), createType(col.type()), col.nullable()))
            .forEach(c -> (keyColNames.contains(c.name()) ? keyCols : valCols).add(c));

        return new SchemaDescriptor(
            ver,
            keyCols.toArray(Column[]::new),
            pkCfg.affinityColumns().value(),
            valCols.toArray(Column[]::new)
        );
    }

    /**
     * Create type from config.
     *
     * @param type Type view.
     * @return Native type.
     */
    private NativeType createType(ColumnTypeView type) {
        switch (type.type().toLowerCase()) {
            case "byte":
                return NativeType.BYTE;
            case "short":
                return NativeType.SHORT;
            case "int":
                return NativeType.INTEGER;
            case "long":
                return NativeType.LONG;
            case "float":
                return NativeType.FLOAT;
            case "double":
                return NativeType.DOUBLE;
            case "uuid":
                return NativeType.UUID;
            case "bitmask":
                return Bitmask.of(type.length());
            case "string":
                return NativeType.STRING;
            case "bytes":
                return NativeType.BYTES;

            default:
                throw new IllegalStateException("Unsupported column type: " + type.type());
        }
    }

    /**
     * Compares schemas.
     *
     * @param expected Expected schema.
     * @param actual Actual schema.
     * @return {@code True} if schemas are equal, {@code false} otherwise.
     */
    public static boolean equalSchemas(SchemaDescriptor expected, SchemaDescriptor actual) {
        if (expected.keyColumns().length() != actual.keyColumns().length() ||
            expected.valueColumns().length() != actual.valueColumns().length())
            return false;

        for (int i = 0; i < expected.length(); i++) {
            if (!expected.column(i).equals(actual.column(i)))
                return false;
        }

        return true;
    }

    /**
     * @param tableId Table id.
     * @return Schema registry for the table.
     */
    public SchemaRegistry schemaRegistryForTable(UUID tableId) {
        final SchemaRegistry reg = schemes.get(tableId);

        if (reg == null)
            throw new SchemaRegistryException("No schema was ever registeref for the table: " + tableId);

        return reg;
    }

    /**
     * Registers new schema.
     *
     * @param tableId Table identifier.
     * @param desc Schema descriptor.
     */
    public CompletableFuture<Boolean> registerSchema(UUID tableId, SchemaDescriptor desc) {
        int schemaVersion = desc.version();

        final Key key = new Key(INTERNAL_PREFIX + tableId + '.' + schemaVersion);

        return metaStorageMgr.invoke(
            Conditions.key(key).value().eq(null),
            Operations.put(key, ByteUtils.toBytes(desc)), //TODO: IGNITE-14679 Serialize schema.
            Operations.noop());
    }

    /**
     * Unregistered all schemas associated with a table identifier.
     *
     * @param tableId Table identifier.
     * @return Future which will complete when all versions of schema will be unregistered.
     */
    public CompletableFuture<Boolean> unregisterSchemas(UUID tableId) {
        List<CompletableFuture<?>> futs = new ArrayList<>();

        String schemaPrefix = INTERNAL_PREFIX + tableId + INTERNAL_VER_SUFFIX;

        try (Cursor<Entry> cursor = metaStorageMgr.range(new Key(schemaPrefix), null)) {
            cursor.forEach(entry ->
                futs.add(metaStorageMgr.remove(entry.key())));
        }
        catch (Exception e) {
            LOG.error("Culdn't remove schemas for the table [tblId=" + tableId + ']');
        }

        return CompletableFuture.allOf(futs.toArray(CompletableFuture[]::new))
            .thenCompose(v -> metaStorageMgr.remove(new Key(INTERNAL_PREFIX + tableId)))
            .thenApply(v -> true);
    }
}
