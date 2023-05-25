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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.ignite.internal.catalog.commands.AlterTableAddColumnParams;
import org.apache.ignite.internal.catalog.commands.AlterTableDropColumnParams;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.commands.CreateTableParams;
import org.apache.ignite.internal.catalog.commands.DropTableParams;
import org.apache.ignite.internal.catalog.descriptors.IndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.SchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.TableDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.CreateTableEventParameters;
import org.apache.ignite.internal.catalog.events.DropTableEventParameters;
import org.apache.ignite.internal.catalog.storage.DropTableEntry;
import org.apache.ignite.internal.catalog.storage.NewTableEntry;
import org.apache.ignite.internal.catalog.storage.ObjectIdGenUpdateEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;
import org.apache.ignite.internal.catalog.storage.UpdateLog;
import org.apache.ignite.internal.catalog.storage.UpdateLog.OnUpdateHandler;
import org.apache.ignite.internal.catalog.storage.VersionedUpdate;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.Producer;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.apache.ignite.lang.TableNotFoundException;
import org.jetbrains.annotations.Nullable;

/**
 * Catalog service implementation.
 * TODO: IGNITE-19081 Introduce catalog events and make CatalogServiceImpl extends Producer.
 */
public class CatalogServiceImpl extends Producer<CatalogEvent, CatalogEventParameters> implements CatalogManager {
    private static final int MAX_RETRY_COUNT = 10;

    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(CatalogServiceImpl.class);

    /** Versioned catalog descriptors. */
    private final NavigableMap<Integer, Catalog> catalogByVer = new ConcurrentSkipListMap<>();

    /** Versioned catalog descriptors sorted in chronological order. */
    private final NavigableMap<Long, Catalog> catalogByTs = new ConcurrentSkipListMap<>();

    private final UpdateLog updateLog;

    private final PendingComparableValuesTracker<Integer, Void> versionTracker = new PendingComparableValuesTracker<>(0);

    /**
     * Constructor.
     */
    public CatalogServiceImpl(UpdateLog updateLog) {
        this.updateLog = updateLog;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        int objectIdGen = 0;

        SchemaDescriptor schemaPublic = new SchemaDescriptor(objectIdGen++, "PUBLIC", 0, new TableDescriptor[0], new IndexDescriptor[0]);
        registerCatalog(new Catalog(0, 0L, objectIdGen, schemaPublic));

        updateLog.registerUpdateHandler(new OnUpdateHandlerImpl());

        updateLog.start();
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        updateLog.stop();
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
        Catalog catalog = catalog(version);

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

    private Catalog catalog(int version) {
        return catalogByVer.get(version);
    }

    private Catalog catalogAt(long timestamp) {
        Entry<Long, Catalog> entry = catalogByTs.floorEntry(timestamp);

        if (entry == null) {
            throw new IllegalStateException("No valid schema found for given timestamp: " + timestamp);
        }

        return entry.getValue();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> createTable(CreateTableParams params) {
        return saveUpdate(catalog -> {
            String schemaName = Objects.requireNonNullElse(params.schemaName(), CatalogService.PUBLIC);

            SchemaDescriptor schema = Objects.requireNonNull(catalog.schema(schemaName), "No schema found: " + schemaName);

            if (schema.table(params.tableName()) != null) {
                throw new TableAlreadyExistsException(schemaName, params.tableName());
            }

            TableDescriptor table = CatalogUtils.fromParams(catalog.objectIdGenState(), params);

            return List.of(
                    new NewTableEntry(table),
                    new ObjectIdGenUpdateEntry(1)
            );
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> dropTable(DropTableParams params) {
        return saveUpdate(catalog -> {
            String schemaName = Objects.requireNonNullElse(params.schemaName(), CatalogService.PUBLIC);

            SchemaDescriptor schema = Objects.requireNonNull(catalog.schema(schemaName), "No schema found: " + schemaName);

            TableDescriptor table = schema.table(params.tableName());

            if (table == null) {
                throw new TableNotFoundException(schemaName, params.tableName());
            }

            return List.of(
                    new DropTableEntry(table.id())
            );
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> addColumn(AlterTableAddColumnParams params) {
        return failedFuture(new UnsupportedOperationException("Not implemented yet."));
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> dropColumn(AlterTableDropColumnParams params) {
        return failedFuture(new UnsupportedOperationException("Not implemented yet."));
    }

    private void registerCatalog(Catalog newCatalog) {
        catalogByVer.put(newCatalog.version(), newCatalog);
        catalogByTs.put(newCatalog.time(), newCatalog);
    }

    private CompletableFuture<Void> saveUpdate(UpdateProducer updateProducer) {
        return saveUpdate(updateProducer, 0);
    }

    private CompletableFuture<Void> saveUpdate(UpdateProducer updateProducer, int attemptNo) {
        if (attemptNo >= MAX_RETRY_COUNT) {
            return failedFuture(new IgniteInternalException(Common.INTERNAL_ERR, "Max retry limit exceeded: " + attemptNo));
        }

        Catalog catalog = catalogByVer.lastEntry().getValue();

        List<UpdateEntry> updates;
        try {
            updates = updateProducer.get(catalog);
        } catch (Exception ex) {
            return failedFuture(ex);
        }

        if (updates.isEmpty()) {
            return completedFuture(null);
        }

        int newVersion = catalog.version() + 1;

        return updateLog.append(new VersionedUpdate(newVersion, updates))
                .thenCompose(result -> versionTracker.waitFor(newVersion).thenApply(none -> result))
                .thenCompose(result -> {
                    if (result) {
                        return completedFuture(null);
                    }

                    return saveUpdate(updateProducer, attemptNo + 1);
                });
    }

    class OnUpdateHandlerImpl implements OnUpdateHandler {
        @Override
        public void handle(VersionedUpdate update) {
            int version = update.version();
            Catalog catalog = catalogByVer.get(version - 1);

            assert catalog != null;

            List<CompletableFuture<?>> eventFutures = new ArrayList<>(update.entries().size());

            for (UpdateEntry entry : update.entries()) {
                String schemaName = CatalogService.PUBLIC;
                SchemaDescriptor schema = Objects.requireNonNull(catalog.schema(schemaName), "No schema found: " + schemaName);

                if (entry instanceof NewTableEntry) {
                    catalog = new Catalog(
                            version,
                            System.currentTimeMillis(),
                            catalog.objectIdGenState(),
                            new SchemaDescriptor(
                                    schema.id(),
                                    schema.name(),
                                    version,
                                    ArrayUtils.concat(schema.tables(), ((NewTableEntry) entry).descriptor()),
                                    schema.indexes()
                            )
                    );

                    eventFutures.add(fireEvent(
                            CatalogEvent.TABLE_CREATE,
                            new CreateTableEventParameters(version, ((NewTableEntry) entry).descriptor())
                    ));

                } else if (entry instanceof DropTableEntry) {
                    int tableId = ((DropTableEntry) entry).tableId();

                    catalog = new Catalog(
                            version,
                            System.currentTimeMillis(),
                            catalog.objectIdGenState(),
                            new SchemaDescriptor(
                                    schema.id(),
                                    schema.name(),
                                    version,
                                    Arrays.stream(schema.tables()).filter(t -> t.id() != tableId).toArray(TableDescriptor[]::new),
                                    schema.indexes()
                            )
                    );

                    eventFutures.add(fireEvent(
                            CatalogEvent.TABLE_DROP,
                            new DropTableEventParameters(version, tableId)
                    ));

                } else if (entry instanceof ObjectIdGenUpdateEntry) {
                    catalog = new Catalog(
                            version,
                            System.currentTimeMillis(),
                            catalog.objectIdGenState() + ((ObjectIdGenUpdateEntry) entry).delta(),
                            new SchemaDescriptor(
                                    schema.id(),
                                    schema.name(),
                                    version,
                                    schema.tables(),
                                    schema.indexes()
                            )
                    );
                } else {
                    assert false : entry;
                }
            }

            registerCatalog(catalog);

            CompletableFuture.allOf(eventFutures.toArray(CompletableFuture[]::new))
                    .thenRun(() -> versionTracker.update(version, null))
                    .whenComplete((ignore, err) -> {
                        if (err != null) {
                            LOG.warn("Failed to apply catalog update.", err);
                        } else {
                            versionTracker.update(version, null);
                        }
                    });
        }
    }

    @FunctionalInterface
    interface UpdateProducer {
        List<UpdateEntry> get(Catalog catalog);
    }
}
