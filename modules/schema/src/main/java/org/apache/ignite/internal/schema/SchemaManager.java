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
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.configuration.internal.ConfigurationManager;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.schema.registry.SchemaRegistry;
import org.apache.ignite.internal.schema.registry.SchemaRegistryException;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.metastorage.common.Conditions;
import org.apache.ignite.metastorage.common.Cursor;
import org.apache.ignite.metastorage.common.Entry;
import org.apache.ignite.metastorage.common.Key;
import org.apache.ignite.metastorage.common.Operations;
import org.apache.ignite.metastorage.common.WatchEvent;
import org.apache.ignite.metastorage.common.WatchListener;
import org.jetbrains.annotations.NotNull;

/**
 * Schema Manager.
 */
// TODO: IGNITE-14586 Remove @SuppressWarnings when implementation provided.
@SuppressWarnings({"FieldCanBeLocal", "unused"}) public class SchemaManager {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(SchemaManager.class);

    /** Internal prefix for the metasorage. */
    private static final String INTERNAL_PREFIX = "internal.tables.schema.";

    /** Configuration manager in order to handle and listen schema specific configuration. */
    private final ConfigurationManager configurationMgr;

    /** Metastorage manager. */
    private final MetaStorageManager metaStorageManager;

    /** Vault manager. */
    private final VaultManager vaultManager;

    /** Schema. */
    private final Map<UUID, SchemaRegistry> schemes = new ConcurrentHashMap<>();

    /**
     * The constructor.
     *
     * @param configurationMgr Configuration manager.
     * @param metaStorageManager Metastorage manager.
     * @param vaultManager Vault manager.
     */
    public SchemaManager(
        ConfigurationManager configurationMgr,
        MetaStorageManager metaStorageManager,
        VaultManager vaultManager
    ) {
        this.configurationMgr = configurationMgr;
        this.metaStorageManager = metaStorageManager;
        this.vaultManager = vaultManager;

        metaStorageManager.registerWatchByPrefix(new Key(INTERNAL_PREFIX), new WatchListener() {
            @Override public boolean onUpdate(@NotNull Iterable<WatchEvent> events) {
                for (WatchEvent evt : events) {
                    String keyTail = evt.newEntry().key().toString().substring(INTERNAL_PREFIX.length());
                    UUID tblId = UUID.fromString(keyTail.substring(0, keyTail.indexOf('.')));

                    int schemaVer = Integer.parseInt(keyTail.substring(keyTail.indexOf('.'), keyTail.length() - 1));

                    final SchemaRegistry schemaReg = schemes.computeIfAbsent(tblId, (k) -> new SchemaRegistry());

                    //TODO: IGNITE-14077 Drop table should cause a drop schema registry as well.
                    if (evt.newEntry().value() == null)
                        schemaReg.cleanupSchema(schemaVer);
                    else //TODO: IGNITE-14679 Deserialize schema.
                        schemaReg.registerSchema((SchemaDescriptor)ByteUtils.fromBytes(evt.newEntry().value()));
                }

                return true;
            }

            @Override public void onError(@NotNull Throwable e) {
                LOG.error("Faled to notyfy Schema manager.", e);
            }
        });
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
     * Gets a current schema for the table specified.
     *
     * @param tableId Table id.
     * @return Schema.
     */
    public SchemaDescriptor schema(UUID tableId) {
        return schemes.get(tableId).schema();
    }

    /**
     * Gets a schema for specific version.
     *
     * @param tableId Table id.
     * @param ver Schema version.
     * @return Schema.
     */
    public SchemaDescriptor schema(UUID tableId, int ver) {
        final SchemaRegistry reg = schemaRegistryForTable(tableId);

        return reg.schema(ver);
    }

    /**
     * @param tableId Table id.
     * @return Schema registry for the table.
     */
    private SchemaRegistry schemaRegistryForTable(UUID tableId) {
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

        return metaStorageManager.invoke(new Key(INTERNAL_PREFIX
                //Tbale id
                + tableId + '.'
                //Schema version
                + schemaVersion),
            Conditions.value().eq(null),
            Operations.put(ByteUtils.toBytes(desc)), //TODO: IGNITE-14679 Serialize schema.
            Operations.noop());
    }

    /**
     * Unregistered all schemas associated with a table identifier.
     *
     * @param tableId Table identifier.
     * @return Future which will complete when all versions of schema will be unregistered.
     */
    public CompletableFuture<Boolean> unregisterSchemas(UUID tableId) {
        ArrayList<CompletableFuture> futs = new ArrayList<>();

        String schemaPrefix = INTERNAL_PREFIX + tableId;

        try (Cursor<Entry> cursor = metaStorageManager.range(new Key(schemaPrefix + '.'), new Key(schemaPrefix + ('.' + 1)))) {
            cursor.forEach(entry ->
                futs.add(metaStorageManager.remove(entry.key())));
        }
        catch (Exception e) {
            LOG.error("Culdn't remove schemas for the table [tblId=" + tableId + ']');
        }

        return CompletableFuture.allOf(futs.toArray(CompletableFuture[]::new)).thenApply(v -> true);
    }
}
