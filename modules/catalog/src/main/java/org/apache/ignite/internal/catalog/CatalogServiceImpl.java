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
import static org.apache.ignite.lang.ErrorGroups.Sql.UNSUPPORTED_DDL_OPERATION_ERR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.commands.AlterColumnParams;
import org.apache.ignite.internal.catalog.commands.AlterTableAddColumnParams;
import org.apache.ignite.internal.catalog.commands.AlterTableDropColumnParams;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateTableParams;
import org.apache.ignite.internal.catalog.commands.DefaultValue;
import org.apache.ignite.internal.catalog.commands.DropTableParams;
import org.apache.ignite.internal.catalog.descriptors.IndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.SchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.TableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.TableDescriptor;
import org.apache.ignite.internal.catalog.events.AddColumnEventParameters;
import org.apache.ignite.internal.catalog.events.AlterColumnEventParameters;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.CreateTableEventParameters;
import org.apache.ignite.internal.catalog.events.DropColumnEventParameters;
import org.apache.ignite.internal.catalog.events.DropTableEventParameters;
import org.apache.ignite.internal.catalog.storage.AlterColumnEntry;
import org.apache.ignite.internal.catalog.storage.DropColumnsEntry;
import org.apache.ignite.internal.catalog.storage.DropTableEntry;
import org.apache.ignite.internal.catalog.storage.NewColumnsEntry;
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
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.lang.ColumnAlreadyExistsException;
import org.apache.ignite.lang.ColumnNotFoundException;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.sql.SqlException;
import org.jetbrains.annotations.Nullable;

/**
 * Catalog service implementation.
 */
public class CatalogServiceImpl extends Producer<CatalogEvent, CatalogEventParameters> implements CatalogManager {
    private static final int MAX_RETRY_COUNT = 10;
    private static final Map<ColumnType, Set<ColumnType>> ALTER_COLUMN_TYPE_TRANSITIONS = new EnumMap<>(ColumnType.class);

    static {
        ALTER_COLUMN_TYPE_TRANSITIONS.put(ColumnType.INT8, EnumSet.of(ColumnType.INT16, ColumnType.INT32, ColumnType.INT64));
        ALTER_COLUMN_TYPE_TRANSITIONS.put(ColumnType.INT16, EnumSet.of(ColumnType.INT32, ColumnType.INT64));
        ALTER_COLUMN_TYPE_TRANSITIONS.put(ColumnType.INT32, EnumSet.of(ColumnType.INT64));
        ALTER_COLUMN_TYPE_TRANSITIONS.put(ColumnType.FLOAT, EnumSet.of(ColumnType.DOUBLE));
    }

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
        if (params.columns().isEmpty()) {
            return completedFuture(null);
        }

        return saveUpdate(catalog -> {
            String schemaName = Objects.requireNonNullElse(params.schemaName(), CatalogService.PUBLIC);

            SchemaDescriptor schema = Objects.requireNonNull(catalog.schema(schemaName), "No schema found: " + schemaName);

            TableDescriptor table = schema.table(params.tableName());

            if (table == null) {
                throw new TableNotFoundException(schemaName, params.tableName());
            }

            List<TableColumnDescriptor> columnDescriptors = new ArrayList<>();

            for (ColumnParams col : params.columns()) {
                if (table.column(col.name()) != null) {
                    throw new ColumnAlreadyExistsException(col.name());
                }

                columnDescriptors.add(CatalogUtils.fromParams(col));
            }

            return List.of(
                    new NewColumnsEntry(table.id(), columnDescriptors)
            );
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> dropColumn(AlterTableDropColumnParams params) {
        if (params.columns().isEmpty()) {
            return completedFuture(null);
        }

        return saveUpdate(catalog -> {
            String schemaName = Objects.requireNonNullElse(params.schemaName(), CatalogService.PUBLIC);

            SchemaDescriptor schema = Objects.requireNonNull(catalog.schema(schemaName), "No schema found: " + schemaName);

            TableDescriptor table = schema.table(params.tableName());

            if (table == null) {
                throw new TableNotFoundException(schemaName, params.tableName());
            }

            for (String columnName : params.columns()) {
                if (table.column(columnName) == null) {
                    throw new ColumnNotFoundException(columnName);
                }
                if (table.isPrimaryKeyColumn(columnName)) {
                    throw new SqlException(
                            Sql.DROP_IDX_COLUMN_CONSTRAINT_ERR,
                            "Can't drop primary key column: column=" + columnName
                    );
                }
            }

            Arrays.stream(schema.indexes())
                    .filter(index -> index.tableId() == table.id())
                    .forEach(index -> params.columns().stream()
                            .filter(index::hasColumn)
                            .findAny()
                            .ifPresent(columnName -> {
                                throw new SqlException(
                                        Sql.DROP_IDX_COLUMN_CONSTRAINT_ERR,
                                        "Can't drop indexed column: columnName=" + columnName + ", indexName="
                                        + index.name()
                                );
                            }));

            return List.of(
                    new DropColumnsEntry(table.id(), params.columns())
            );
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> alterColumn(AlterColumnParams params) {
        return saveUpdate(catalog -> {
            String schemaName = Objects.requireNonNullElse(params.schemaName(), CatalogService.PUBLIC);

            SchemaDescriptor schema = Objects.requireNonNull(catalog.schema(schemaName), "No schema found: " + schemaName);

            TableDescriptor table = schema.table(params.tableName());

            if (table == null) {
                throw new TableNotFoundException(schemaName, params.tableName());
            }

            String columnName = params.columnName();

            Optional<TableColumnDescriptor> source = table.columns().stream().filter(desc -> desc.name().equals(columnName)).findFirst();

            if (source.isEmpty()) {
                throw new ColumnNotFoundException(columnName);
            }

            TableColumnDescriptor origin = source.get();

            if (table.isPrimaryKeyColumn(origin.name())) {
                if (params.notNull() != null) {
                    throwUnsupportedDdl("Cannot change NOT NULL for the primary key column '{}'.", origin.name());
                }

                if (params.type() != null) {
                    throwUnsupportedDdl("Cannot change data type for primary key column '{}'.", origin.name());
                }
            }

            boolean nullable = !Objects.requireNonNullElse(params.notNull(), !origin.nullable());
            DefaultValue defaultValue = Objects.requireNonNullElse(params.defaultValue(origin.type()), origin.defaultValue());
            ColumnType type = Objects.requireNonNullElse(params.type(), origin.type());
            int scale = Objects.requireNonNullElse(params.scale(), origin.scale());

            boolean varLenType = type == ColumnType.STRING || type == ColumnType.BYTE_ARRAY;
            int precision = varLenType ? origin.precision() : Objects.requireNonNullElse(params.precision(), origin.precision());
            int length = varLenType ? Objects.requireNonNullElse(params.precision(), origin.length()) : origin.length();

            TableColumnDescriptor target = new TableColumnDescriptor(origin.name(), type, nullable, defaultValue, precision, scale, length);

            if (origin.equals(target)) {
                // No modifications required.
                return Collections.emptyList();
            }

            if (origin.nullable() != target.nullable() && origin.nullable()) {
                throwUnsupportedDdl("Cannot set NOT NULL for column '{}'.", origin.name());
            }

            if (origin.scale() != target.scale()) {
                throwUnsupportedDdl("Cannot change scale for column '{}'.", origin.name());
            }

            if (origin.type() != target.type()) {
                Set<ColumnType> supportedTransitions = ALTER_COLUMN_TYPE_TRANSITIONS.get(origin.type());

                if (supportedTransitions == null || !supportedTransitions.contains(params.type())) {
                    throwUnsupportedDdl(
                            "Cannot change data type for column '{}' [from={}, to={}].", origin.name(), origin.type(), target.type());
                }
            }

            if (target.length() < origin.length()) {
                throwUnsupportedDdl("Cannot decrease length to {} for column '{}'.", target.length(), origin.name());
            }

            if (origin.precision() != target.precision()) {
                if (target.type() != ColumnType.DECIMAL) {
                    throwUnsupportedDdl("Cannot change precision for column '{}'.", origin.name());
                } else if (target.precision() < origin.precision()) {
                    throwUnsupportedDdl("Cannot decrease precision to {} for column '{}'.", params.precision(), origin.name());
                }
            }

            return List.of(new AlterColumnEntry(table.id(), target));
        });
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
                } else if (entry instanceof NewColumnsEntry) {
                    int tableId = ((NewColumnsEntry) entry).tableId();
                    List<TableColumnDescriptor> columnDescriptors = ((NewColumnsEntry) entry).descriptors();

                    catalog = new Catalog(
                            version,
                            System.currentTimeMillis(),
                            catalog.objectIdGenState(),
                            new SchemaDescriptor(
                                    schema.id(),
                                    schema.name(),
                                    version,
                                    Arrays.stream(schema.tables())
                                            .map(table -> table.id() != tableId
                                                    ? table
                                                    : new TableDescriptor(
                                                            table.id(),
                                                            table.name(),
                                                            CollectionUtils.concat(table.columns(), columnDescriptors),
                                                            table.primaryKeyColumns(),
                                                            table.colocationColumns())
                                            )
                                            .toArray(TableDescriptor[]::new),
                                    schema.indexes()
                            )
                    );

                    eventFutures.add(fireEvent(
                            CatalogEvent.TABLE_ALTER,
                            new AddColumnEventParameters(version, tableId, columnDescriptors)
                    ));
                } else if (entry instanceof DropColumnsEntry) {
                    int tableId = ((DropColumnsEntry) entry).tableId();
                    Set<String> columns = ((DropColumnsEntry) entry).columns();

                    catalog = new Catalog(
                            version,
                            System.currentTimeMillis(),
                            catalog.objectIdGenState(),
                            new SchemaDescriptor(
                                    schema.id(),
                                    schema.name(),
                                    version,
                                    Arrays.stream(schema.tables())
                                            .map(table -> table.id() != tableId
                                                    ? table
                                                    : new TableDescriptor(
                                                            table.id(),
                                                            table.name(),
                                                            table.columns().stream().filter(col -> !columns.contains(col.name()))
                                                                    .collect(Collectors.toList()),
                                                            table.primaryKeyColumns(),
                                                            table.colocationColumns())
                                            )
                                            .toArray(TableDescriptor[]::new),
                                    schema.indexes()
                            )
                    );

                    eventFutures.add(fireEvent(
                            CatalogEvent.TABLE_ALTER,
                            new DropColumnEventParameters(version, tableId, columns)
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
                } else if (entry instanceof AlterColumnEntry) {
                    int tableId = ((AlterColumnEntry) entry).tableId();
                    TableColumnDescriptor target = ((AlterColumnEntry) entry).descriptor();

                    catalog = new Catalog(
                            version,
                            System.currentTimeMillis(),
                            catalog.objectIdGenState(),
                            new SchemaDescriptor(
                                    schema.id(),
                                    schema.name(),
                                    version,
                                    Arrays.stream(schema.tables())
                                            .map(table -> table.id() != tableId
                                                    ? table
                                                    : new TableDescriptor(
                                                            table.id(),
                                                            table.name(),
                                                            table.columns().stream()
                                                                    .map(source -> source.name().equals(target.name())
                                                                            ? target
                                                                            : source).collect(Collectors.toList()),
                                                            table.primaryKeyColumns(),
                                                            table.colocationColumns())
                                            )
                                            .toArray(TableDescriptor[]::new),
                                    schema.indexes()
                            )
                    );

                    eventFutures.add(fireEvent(
                            CatalogEvent.TABLE_ALTER,
                            new AlterColumnEventParameters(version, tableId, target)
                    ));
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

    private static void throwUnsupportedDdl(String msg, Object... params) {
        throw new SqlException(UNSUPPORTED_DDL_OPERATION_ERR, IgniteStringFormatter.format(msg, params));
    }

    @FunctionalInterface
    interface UpdateProducer {
        List<UpdateEntry> get(Catalog catalog);
    }
}
