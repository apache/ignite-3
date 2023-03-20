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
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.catalog.commands.AlterTableAddColumnParams;
import org.apache.ignite.internal.catalog.commands.AlterTableDropColumnParams;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.commands.CreateTableParams;
import org.apache.ignite.internal.catalog.commands.DropTableParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogDescriptor;
import org.apache.ignite.internal.catalog.descriptors.IndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.SchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.TableDescriptor;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.jetbrains.annotations.Nullable;

/**
 * Catalog service implementation.
 */
public class CatalogServiceImpl implements CatalogService, CatalogManager {
    private static final AtomicInteger TABLE_ID_GEN = new AtomicInteger();

    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(CatalogServiceImpl.class);

    /** Versioned catalog descriptors. */
    //TODO: IGNITE-18535 Use copy-on-write approach with IntMap instead??
    private final NavigableMap<Integer, CatalogDescriptor> catalogByVer = new ConcurrentSkipListMap<>();

    /** Versioned catalog descriptors sorted in chronological order. */
    //TODO: IGNITE-18535 Use copy-on-write approach with Map instead??
    private final NavigableMap<Long, CatalogDescriptor> catalogByTs = new ConcurrentSkipListMap<>();

    private final MetaStorageManager metaStorageMgr;

    private final WatchListener catalogVersionsListener;

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
            metaStorageMgr.registerPrefixWatch(ByteArray.fromString("catalog-"), catalogVersionsListener);
        }

        //TODO: IGNITE-18535 restore state.
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
        return catalogAt(timestamp).schema(CatalogService.PUBLIC).table(tableName);
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

        return catalog.schema(CatalogService.PUBLIC);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable SchemaDescriptor activeSchema(long timestamp) {
        return catalogAt(timestamp).schema(CatalogService.PUBLIC);
    }

    private CatalogDescriptor catalog(int version) {
        return catalogByVer.get(version);
    }

    private CatalogDescriptor catalogAt(long timestamp) {
        Entry<Long, CatalogDescriptor> entry = catalogByTs.floorEntry(timestamp);

        if (entry == null) {
            throw new IllegalStateException("No valid schema found for given timestamp: " + timestamp);
        }

        return entry.getValue();
    }

    /**
     * MetaStorage event listener for catalog metadata updates.
     */
    private static class CatalogEventListener implements WatchListener {
        /** {@inheritDoc} */
        @Override
        public String id() {
            return "catalog-history-watch";
        }

        /** {@inheritDoc} */
        @Override
        public CompletableFuture<Void> onUpdate(WatchEvent event) {
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
    public CompletableFuture<?> createTable(CreateTableParams params) {
        // Create operation future, which will returned, and saved to a map.
        //
        // Creates TableDescriptor and saves it to MetaStorage.
        // Atomically:
        //        int id = metaStorage.get("lastId") + 1;
        //        TableDescriptor table = new TableDescriptor(tableId, params)
        //
        //        Catalog newCatalog = catalogByVer.get(id -1).copy()
        //        newCatalog.setId(id).addTable(table);
        //
        //        metaStorage.put("catalog-"+id, new Catalog());
        //        metaStorage.put("lastId", id);
        //
        // Subscribes operation future to the MetaStorage future for failure handling
        // Operation future must be completed when got event from catalog service for expected table.

        // Dummy implementation.
        synchronized (this) {
            CatalogDescriptor catalog = catalogByVer.lastEntry().getValue();

            String schemaName = Objects.requireNonNullElse(params.schemaName(), CatalogService.PUBLIC);

            SchemaDescriptor schema = Objects.requireNonNull(catalog.schema(schemaName), "No schema found: " + schemaName);

            if (schema.table(params.tableName()) != null) {
                return params.ifTableExists()
                        ? completedFuture(false)
                        : failedFuture(new TableAlreadyExistsException(schemaName, params.tableName()));
            }

            int newVersion = catalogByVer.lastKey() + 1;

            //TODO: IGNITE-18535 Fix tableId generation. Make it consistent on all nodes.
            TableDescriptor table = CatalogUtils.fromParams(TABLE_ID_GEN.incrementAndGet(), params);

            CatalogDescriptor newCatalog = new CatalogDescriptor(
                    newVersion,
                    System.currentTimeMillis(),
                    new SchemaDescriptor(
                            schema.id(),
                            schemaName,
                            newVersion,
                            ArrayUtils.concat(schema.tables(), table),
                            schema.indexes()
                    )
            );

            registerCatalog(newCatalog);
        }

        return completedFuture(true);
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
