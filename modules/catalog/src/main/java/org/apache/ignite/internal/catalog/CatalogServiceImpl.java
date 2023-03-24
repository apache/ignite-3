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

package org.apache.ignite.internal.catalog;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import org.apache.ignite.internal.catalog.commands.AlterTableAddColumnParams;
import org.apache.ignite.internal.catalog.commands.AlterTableDropColumnParams;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.commands.CreateTableParams;
import org.apache.ignite.internal.catalog.commands.DropTableParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogDescriptor;
import org.apache.ignite.internal.catalog.descriptors.IndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.SchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.TableDescriptor;
import org.apache.ignite.internal.catalog.events.CreateTableEvent;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Conditions;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.apache.ignite.internal.metastorage.dsl.Statements;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.jetbrains.annotations.Nullable;

/**
 * Catalog service implementation.
 * TODO: IGNITE-19081 Introduce catalog events and make CatalogServiceImpl extends Producer.
 */
public class CatalogServiceImpl implements CatalogService, CatalogManager {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(CatalogServiceImpl.class);
    public static final String CATALOG_VER_PREFIX = "catalog.ver.";

    /** Versioned catalog descriptors. */
    private final NavigableMap<Integer, CatalogDescriptor> catalogByVer = new ConcurrentSkipListMap<>();

    /** Versioned catalog descriptors sorted in chronological order. */
    private final NavigableMap<Long, CatalogDescriptor> catalogByTs = new ConcurrentSkipListMap<>();

    private final MetaStorageManager metaStorageMgr;

    private final WatchListener catalogVersionsListener;

    private final ExecutorService executorService = ForkJoinPool.commonPool();

    private final ConcurrentMap<UUID, CompletableFuture<Boolean>> futMap = new ConcurrentHashMap<>();

    /**
     * Constructor.
     */
    public CatalogServiceImpl(MetaStorageManager metaStorageMgr) {
        this.metaStorageMgr = metaStorageMgr;
        catalogVersionsListener = new CatalogEventListener();
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        if (CatalogService.useCatalogService()) {
            metaStorageMgr.registerPrefixWatch(ByteArray.fromString(CATALOG_VER_PREFIX), catalogVersionsListener);
        }

        //TODO: IGNITE-19080 restore state.
        registerCatalog(new CatalogDescriptor(0, 0L,
                new SchemaDescriptor(0, "PUBLIC", 0, new TableDescriptor[0], new IndexDescriptor[0])));
    }

    /** {@inheritDoc} */
    @Override
    public void stop() {
        metaStorageMgr.unregisterWatch(catalogVersionsListener);
    }

    /** {@inheritDoc} */
    @Override
    public TableDescriptor table(String tableName, long timestamp) {
        return catalogAt(timestamp).table(CatalogUtils.DEFAULT_SCHEMA, tableName);
    }

    /** {@inheritDoc} */
    @Override
    public TableDescriptor table(int tableId, long timestamp) {
        return catalogAt(timestamp).table(tableId);
    }

    /** {@inheritDoc} */
    @Override
    public IndexDescriptor index(int indexId, long timestamp) {
        return catalogAt(timestamp).index(indexId);
    }

    /** {@inheritDoc} */
    @Override
    public Collection<IndexDescriptor> tableIndexes(int tableId, long timestamp) {
        return catalogAt(timestamp).tableIndexes(tableId);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable SchemaDescriptor schema(int version) {
        CatalogDescriptor catalog = catalog(version);

        if (catalog == null) {
            return null;
        }

        return catalog.schema(CatalogUtils.DEFAULT_SCHEMA);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable SchemaDescriptor activeSchema(long timestamp) {
        return catalogAt(timestamp).schema(CatalogUtils.DEFAULT_SCHEMA);
    }

    private CatalogDescriptor catalog(int version) {
        return catalogByVer.get(version);
    }

    private CatalogDescriptor catalogAt(long timestamp) {
        Map.Entry<Long, CatalogDescriptor> entry = catalogByTs.floorEntry(timestamp);

        if (entry == null) {
            throw new IllegalStateException("No valid schema found for given timestamp: " + timestamp);
        }

        return entry.getValue();
    }

    /**
     * MetaStorage event listener for catalog metadata updates.
     */
    private class CatalogEventListener implements WatchListener {
        /** {@inheritDoc} */
        @Override
        public String id() {
            return "catalog-history-watch";
        }

        /** {@inheritDoc} */
        @Override
        public CompletableFuture<Void> onUpdate(WatchEvent watchEvent) {
            assert watchEvent.single();

            EntryEvent entryEvent = watchEvent.entryEvent();

            if (entryEvent.newEntry().value() != null) {
                assert entryEvent.oldEntry().empty();

                CreateTableEvent event = ByteUtils.fromBytes(entryEvent.newEntry().value());

                TableDescriptor tableDesc = event.tableDescriptor();

                int catalogId = event.catalogId();

                CatalogDescriptor catalog = catalogByVer.get(catalogId - 1);

                SchemaDescriptor schema = catalog.schema(tableDesc.schemaName());

                CatalogDescriptor newCatalog = new CatalogDescriptor(
                        catalogId,
                        System.currentTimeMillis(),
                        new SchemaDescriptor(
                                schema.id(),
                                schema.name(),
                                catalogId,
                                ArrayUtils.concat(schema.tables(), tableDesc),
                                schema.indexes()
                        )
                );

                registerCatalog(newCatalog);

                // Notify listeners.

                CompletableFuture<Boolean> rmv = futMap.remove(event.opUid());

                if (rmv != null) {
                    rmv.completeAsync(() -> true, executorService);
                }
            }

            return completedFuture(null);
        }

        /** {@inheritDoc} */
        @Override
        public void onError(Throwable e) {
            LOG.warn("Unable to process catalog update event", e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Boolean> createTable(CreateTableParams params) {
        CompletableFuture<Boolean> opFut = new CompletableFuture<>();

        UUID uuid = UUID.randomUUID();

        futMap.put(uuid, opFut);

        createTableInternal(uuid, params)
                .whenCompleteAsync((res, err) -> {
                    if (err == null && res) {
                        return;
                    }

                    futMap.remove(uuid, opFut);

                    if (err != null) {
                        opFut.completeExceptionally(err);
                    } else if (params.ifTableExists()) {
                        opFut.complete(false);
                    } else {
                        opFut.completeExceptionally(new TableAlreadyExistsException(params.schemaName(), params.tableName()));
                    }
                });

        return opFut;
    }

    /**
     * Creates then save operation meta information to the MetaStorage.
     *
     * <pre>Atomically:
     *     int newVer = metaStorage.get("lastVer") + 1;
     *
     *     validate(params);
     *     Object diff = createDiff(catalog, params);
     *
     *     metaStorage.put("catalog.ver." + newVer, diff);
     *     metaStorage.put("lastVer", newVer);
     * </pre>
     */
    private CompletableFuture<Boolean> createTableInternal(UUID opUid, CreateTableParams params) {
        ByteArray LAST_VER_KEY = ByteArray.fromString("catalog.lastVer");
        ByteArray TABLE_ID_KEY = ByteArray.fromString("catalog.tableId");

        return metaStorageMgr.getAll(Set.of(LAST_VER_KEY, TABLE_ID_KEY))
                .thenComposeAsync(entries -> {
                    Entry lastVerEntry = entries.get(LAST_VER_KEY);
                    Entry tableIdEntry = entries.get(TABLE_ID_KEY);

                    int lastVer = lastVerEntry.empty() ? 0 : ByteUtils.bytesToInt(lastVerEntry.value());
                    int tableId = tableIdEntry.empty() ? 0 : ByteUtils.bytesToInt(tableIdEntry.value());

                    int newVer = lastVer + 1;
                    int newTableId = tableId + 1;

                    CatalogDescriptor catalog = catalogByVer.get(lastVer);

                    assert catalog.table(newTableId) == null;

                    TableDescriptor tableDesc = CatalogUtils.fromParams(newTableId, params);
                    // params.validate(catalog); ???
                    // validate(catalog, table);

                    if (catalog.table(tableDesc.schemaName(), tableDesc.name()) != null) {
                        return completedFuture(false);
                    }

                    CreateTableEvent createTableEvent = new CreateTableEvent(opUid, newVer, tableDesc);

                    return metaStorageMgr.invoke(
                            Statements.iif(
                                    Conditions.value(LAST_VER_KEY).eq(lastVerEntry.value()),
                                    Operations.ops(
                                            Operations.put(LAST_VER_KEY, ByteUtils.intToBytes(newVer)),
                                            Operations.put(TABLE_ID_KEY, ByteUtils.intToBytes(newTableId)),
                                            Operations.put(ByteArray.fromString(CATALOG_VER_PREFIX + newVer),
                                                    ByteUtils.toBytes(createTableEvent))
                                    ).yield(true),
                                    Operations.ops().yield(false)
                            )
                    ).thenComposeAsync(
                            res -> res.getAsBoolean() ? completedFuture(true) : createTableInternal(opUid, params),
                            executorService
                    );
                }, executorService);
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<?> dropTable(DropTableParams params) {
        return failedFuture(new UnsupportedOperationException("Not implemented yet."));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<?> addColumn(AlterTableAddColumnParams params) {
        return failedFuture(new UnsupportedOperationException("Not implemented yet."));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<?> dropColumn(AlterTableDropColumnParams params) {
        return failedFuture(new UnsupportedOperationException("Not implemented yet."));
    }

    private void registerCatalog(CatalogDescriptor newCatalog) {
        catalogByVer.put(newCatalog.version(), newCatalog);
        catalogByTs.put(newCatalog.time(), newCatalog);
    }
}
