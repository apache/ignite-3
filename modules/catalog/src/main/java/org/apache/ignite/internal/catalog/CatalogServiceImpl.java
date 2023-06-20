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
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.commands.CreateZoneParams.INFINITE_TIMER_VALUE;
import static org.apache.ignite.lang.ErrorGroups.Sql.UNSUPPORTED_DDL_OPERATION_ERR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.commands.AlterColumnParams;
import org.apache.ignite.internal.catalog.commands.AlterTableAddColumnParams;
import org.apache.ignite.internal.catalog.commands.AlterTableDropColumnParams;
import org.apache.ignite.internal.catalog.commands.AlterZoneParams;
import org.apache.ignite.internal.catalog.commands.CatalogUtils;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateHashIndexParams;
import org.apache.ignite.internal.catalog.commands.CreateSortedIndexParams;
import org.apache.ignite.internal.catalog.commands.CreateTableParams;
import org.apache.ignite.internal.catalog.commands.CreateZoneParams;
import org.apache.ignite.internal.catalog.commands.DropIndexParams;
import org.apache.ignite.internal.catalog.commands.DropTableParams;
import org.apache.ignite.internal.catalog.commands.DropZoneParams;
import org.apache.ignite.internal.catalog.commands.RenameZoneParams;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableColumnDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.AddColumnEventParameters;
import org.apache.ignite.internal.catalog.events.AlterColumnEventParameters;
import org.apache.ignite.internal.catalog.events.AlterZoneEventParameters;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.CreateIndexEventParameters;
import org.apache.ignite.internal.catalog.events.CreateTableEventParameters;
import org.apache.ignite.internal.catalog.events.CreateZoneEventParameters;
import org.apache.ignite.internal.catalog.events.DropColumnEventParameters;
import org.apache.ignite.internal.catalog.events.DropIndexEventParameters;
import org.apache.ignite.internal.catalog.events.DropTableEventParameters;
import org.apache.ignite.internal.catalog.events.DropZoneEventParameters;
import org.apache.ignite.internal.catalog.storage.AlterColumnEntry;
import org.apache.ignite.internal.catalog.storage.AlterZoneEntry;
import org.apache.ignite.internal.catalog.storage.DropColumnsEntry;
import org.apache.ignite.internal.catalog.storage.DropIndexEntry;
import org.apache.ignite.internal.catalog.storage.DropTableEntry;
import org.apache.ignite.internal.catalog.storage.DropZoneEntry;
import org.apache.ignite.internal.catalog.storage.NewColumnsEntry;
import org.apache.ignite.internal.catalog.storage.NewIndexEntry;
import org.apache.ignite.internal.catalog.storage.NewTableEntry;
import org.apache.ignite.internal.catalog.storage.NewZoneEntry;
import org.apache.ignite.internal.catalog.storage.ObjectIdGenUpdateEntry;
import org.apache.ignite.internal.catalog.storage.UpdateEntry;
import org.apache.ignite.internal.catalog.storage.UpdateLog;
import org.apache.ignite.internal.catalog.storage.UpdateLog.OnUpdateHandler;
import org.apache.ignite.internal.catalog.storage.VersionedUpdate;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.Producer;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.lang.ColumnAlreadyExistsException;
import org.apache.ignite.lang.ColumnNotFoundException;
import org.apache.ignite.lang.DistributionZoneAlreadyExistsException;
import org.apache.ignite.lang.DistributionZoneBindTableException;
import org.apache.ignite.lang.DistributionZoneNotFoundException;
import org.apache.ignite.lang.ErrorGroups;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.ErrorGroups.DistributionZones;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.IndexAlreadyExistsException;
import org.apache.ignite.lang.IndexNotFoundException;
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

    /** Safe time to wait before new Catalog version activation. */
    private static final int DEFAULT_DELAY_DURATION = 0;

    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(CatalogServiceImpl.class);

    /** Versioned catalog descriptors. */
    private final NavigableMap<Integer, Catalog> catalogByVer = new ConcurrentSkipListMap<>();

    /** Versioned catalog descriptors sorted in chronological order. */
    private final NavigableMap<Long, Catalog> catalogByTs = new ConcurrentSkipListMap<>();

    private final UpdateLog updateLog;

    private final PendingComparableValuesTracker<Integer, Void> versionTracker = new PendingComparableValuesTracker<>(0);

    private final HybridClock clock;

    private final LongSupplier delayDurationMsSupplier;
    private volatile long delayDurationMs;

    /**
     * Constructor.
     */
    public CatalogServiceImpl(UpdateLog updateLog, HybridClock clock) {
        this(updateLog, clock, DEFAULT_DELAY_DURATION);
    }

    /**
     * Constructor.
     */
    CatalogServiceImpl(UpdateLog updateLog, HybridClock clock, long delayDurationMs) {
        this(updateLog, clock, () -> delayDurationMs);
    }

    /**
     * Constructor.
     */
    public CatalogServiceImpl(UpdateLog updateLog, HybridClock clock, LongSupplier delayDurationMsSupplier) {
        this.updateLog = updateLog;
        this.clock = clock;
        this.delayDurationMsSupplier = delayDurationMsSupplier;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        delayDurationMs = delayDurationMsSupplier.getAsLong();

        int objectIdGen = 0;

        // TODO: IGNITE-19082 Move default schema objects initialization to cluster init procedure.
        CatalogSchemaDescriptor schemaPublic = new CatalogSchemaDescriptor(
                objectIdGen++,
                "PUBLIC",
                new CatalogTableDescriptor[0],
                new CatalogIndexDescriptor[0]
        );
        CatalogZoneDescriptor defaultZone = new CatalogZoneDescriptor(
                objectIdGen++,
                CatalogService.DEFAULT_ZONE_NAME,
                25,
                1,
                INFINITE_TIMER_VALUE,
                INFINITE_TIMER_VALUE,
                INFINITE_TIMER_VALUE,
                CreateZoneParams.DEFAULT_FILTER
        );
        registerCatalog(new Catalog(0, 0L, objectIdGen, List.of(defaultZone), List.of(schemaPublic)));

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
    public CatalogTableDescriptor table(String tableName, long timestamp) {
        return catalogAt(timestamp).schema(CatalogService.PUBLIC).table(tableName);
    }

    /** {@inheritDoc} */
    @Override
    public CatalogTableDescriptor table(int tableId, long timestamp) {
        return catalogAt(timestamp).table(tableId);
    }

    /** {@inheritDoc} */
    @Override
    public CatalogIndexDescriptor index(String indexName, long timestamp) {
        return catalogAt(timestamp).schema(CatalogService.PUBLIC).index(indexName);
    }

    /** {@inheritDoc} */
    @Override
    public CatalogIndexDescriptor index(int indexId, long timestamp) {
        return catalogAt(timestamp).index(indexId);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable CatalogSchemaDescriptor schema(int version) {
        Catalog catalog = catalog(version);

        if (catalog == null) {
            return null;
        }

        return catalog.schema(CatalogService.PUBLIC);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable CatalogSchemaDescriptor schema(String schemaName, int version) {
        Catalog catalog = catalog(version);

        if (catalog == null) {
            return null;
        }

        return catalog.schema(schemaName == null ? CatalogService.PUBLIC : schemaName);
    }

    /** {@inheritDoc} */
    @Override
    public CatalogZoneDescriptor zone(String zoneName, long timestamp) {
        return catalogAt(timestamp).zone(zoneName);
    }

    /** {@inheritDoc} */
    @Override
    public CatalogZoneDescriptor zone(int zoneId, long timestamp) {
        return catalogAt(timestamp).zone(zoneId);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable CatalogSchemaDescriptor activeSchema(long timestamp) {
        return catalogAt(timestamp).schema(CatalogService.PUBLIC);
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable CatalogSchemaDescriptor activeSchema(String schemaName, long timestamp) {
        return catalogAt(timestamp).schema(schemaName == null ? CatalogService.PUBLIC : schemaName);
    }

    /** {@inheritDoc} */
    @Override
    public int activeCatalogVersion(long timestamp) {
        return catalogAt(timestamp).version();
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

            CatalogSchemaDescriptor schema = Objects.requireNonNull(catalog.schema(schemaName), "No schema found: " + schemaName);

            if (schema.table(params.tableName()) != null) {
                throw new TableAlreadyExistsException(schemaName, params.tableName());
            }

            params.columns().stream().map(ColumnParams::name).filter(Predicate.not(new HashSet<>()::add))
                    .findAny().ifPresent(columnName -> {
                        throw new IgniteInternalException(
                                ErrorGroups.Index.INVALID_INDEX_DEFINITION_ERR, "Can't create table with duplicate columns: "
                                + params.columns().stream().map(ColumnParams::name).collect(Collectors.joining(", "))
                        );
                    });

            String zoneName = Objects.requireNonNullElse(params.zone(), CatalogService.DEFAULT_ZONE_NAME);

            CatalogZoneDescriptor zone = Objects.requireNonNull(catalog.zone(zoneName), "No zone found: " + zoneName);

            CatalogTableDescriptor table = CatalogUtils.fromParams(catalog.objectIdGenState(), zone.id(), params);

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

            CatalogSchemaDescriptor schema = Objects.requireNonNull(catalog.schema(schemaName), "No schema found: " + schemaName);

            CatalogTableDescriptor table = schema.table(params.tableName());

            if (table == null) {
                throw new TableNotFoundException(schemaName, params.tableName());
            }

            List<UpdateEntry> updateEntries = new ArrayList<>();

            Arrays.stream(schema.indexes())
                    .filter(index -> index.tableId() == table.id())
                    .forEach(index -> updateEntries.add(new DropIndexEntry(index.id())));

            updateEntries.add(new DropTableEntry(table.id()));

            return updateEntries;
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

            CatalogSchemaDescriptor schema = Objects.requireNonNull(catalog.schema(schemaName), "No schema found: " + schemaName);

            CatalogTableDescriptor table = schema.table(params.tableName());

            if (table == null) {
                throw new TableNotFoundException(schemaName, params.tableName());
            }

            List<CatalogTableColumnDescriptor> columnDescriptors = new ArrayList<>();

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

            CatalogSchemaDescriptor schema = Objects.requireNonNull(catalog.schema(schemaName), "No schema found: " + schemaName);

            CatalogTableDescriptor table = schema.table(params.tableName());

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
                                        "Can't drop indexed column: columnName=" + columnName + ", indexName=" + index.name()
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

            CatalogSchemaDescriptor schema = Objects.requireNonNull(catalog.schema(schemaName), "No schema found: " + schemaName);

            CatalogTableDescriptor table = schema.table(params.tableName());

            if (table == null) {
                throw new TableNotFoundException(schemaName, params.tableName());
            }

            String columnName = params.columnName();

            CatalogTableColumnDescriptor origin = table.columns().stream()
                    .filter(desc -> desc.name().equals(columnName))
                    .findFirst()
                    .orElseThrow(() -> new ColumnNotFoundException(columnName));

            CatalogTableColumnDescriptor target = new CatalogTableColumnDescriptor(
                    origin.name(),
                    Objects.requireNonNullElse(params.type(), origin.type()),
                    !Objects.requireNonNullElse(params.notNull(), !origin.nullable()),
                    Objects.requireNonNullElse(params.precision(), origin.precision()),
                    Objects.requireNonNullElse(params.scale(), origin.scale()),
                    Objects.requireNonNullElse(params.length(), origin.length()),
                    Objects.requireNonNullElse(params.defaultValue(origin.type()), origin.defaultValue())
            );

            if (origin.equals(target)) {
                // No modifications required.
                return Collections.emptyList();
            }

            boolean isPkColumn = table.isPrimaryKeyColumn(origin.name());

            if (origin.nullable() != target.nullable()) {
                if (isPkColumn) {
                    throwUnsupportedDdl("Cannot change NOT NULL for the primary key column '{}'.", origin.name());
                }

                if (origin.nullable()) {
                    throwUnsupportedDdl("Cannot set NOT NULL for column '{}'.", origin.name());
                }
            }

            if (origin.scale() != target.scale()) {
                throwUnsupportedDdl("Cannot change scale for column '{}'.", origin.name());
            }

            if (origin.type() != target.type()) {
                if (isPkColumn) {
                    throwUnsupportedDdl("Cannot change data type for primary key column '{}'.", origin.name());
                }

                if (!CatalogUtils.isSupportedColumnTypeChange(origin.type(), target.type())) {
                    throwUnsupportedDdl("Cannot change data type for column '{}' [from={}, to={}].",
                            origin.name(), origin.type(), target.type());
                }
            }

            if (origin.length() != target.length() && target.type() != ColumnType.STRING && target.type() != ColumnType.BYTE_ARRAY) {
                throwUnsupportedDdl("Cannot change length for column '{}'.", origin.name());
            } else if (target.length() < origin.length()) {
                throwUnsupportedDdl("Cannot decrease length to {} for column '{}'.", target.length(), origin.name());
            }

            if (origin.precision() != target.precision() && target.type() != ColumnType.DECIMAL) {
                throwUnsupportedDdl("Cannot change precision for column '{}'.", origin.name());
            } else if (target.precision() < origin.precision()) {
                throwUnsupportedDdl("Cannot decrease precision to {} for column '{}'.", params.precision(), origin.name());
            }

            return List.of(new AlterColumnEntry(table.id(), target));
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> createIndex(CreateHashIndexParams params) {
        return saveUpdate(catalog -> {
            String schemaName = Objects.requireNonNullElse(params.schemaName(), CatalogService.PUBLIC);

            CatalogSchemaDescriptor schema = Objects.requireNonNull(catalog.schema(schemaName), "No schema found: " + schemaName);

            if (schema.index(params.indexName()) != null) {
                throw new IndexAlreadyExistsException(schemaName, params.indexName());
            }

            CatalogTableDescriptor table = schema.table(params.tableName());

            if (table == null) {
                throw new TableNotFoundException(schemaName, params.tableName());
            }

            if (params.columns().isEmpty()) {
                throw new IgniteInternalException(
                        ErrorGroups.Index.INVALID_INDEX_DEFINITION_ERR,
                        "No index columns was specified."
                );
            }

            Predicate<String> duplicateValidator = Predicate.not(new HashSet<>()::add);

            for (String columnName : params.columns()) {
                CatalogTableColumnDescriptor columnDescriptor = table.columnDescriptor(columnName);

                if (columnDescriptor == null) {
                    throw new ColumnNotFoundException(columnName);
                } else if (duplicateValidator.test(columnName)) {
                    throw new IgniteInternalException(
                            ErrorGroups.Index.INVALID_INDEX_DEFINITION_ERR,
                            "Can't create index on duplicate columns: " + String.join(", ", params.columns())
                    );
                }
            }

            CatalogIndexDescriptor index = CatalogUtils.fromParams(catalog.objectIdGenState(), table.id(), params);

            return List.of(
                    new NewIndexEntry(index),
                    new ObjectIdGenUpdateEntry(1)
            );
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> createIndex(CreateSortedIndexParams params) {
        return saveUpdate(catalog -> {
            String schemaName = Objects.requireNonNullElse(params.schemaName(), CatalogService.PUBLIC);

            CatalogSchemaDescriptor schema = Objects.requireNonNull(catalog.schema(schemaName), "No schema found: " + schemaName);

            if (schema.index(params.indexName()) != null) {
                throw new IndexAlreadyExistsException(schemaName, params.indexName());
            }

            CatalogTableDescriptor table = schema.table(params.tableName());

            if (table == null) {
                throw new TableNotFoundException(schemaName, params.tableName());
            }

            if (params.columns().isEmpty()) {
                throw new IgniteInternalException(
                        ErrorGroups.Index.INVALID_INDEX_DEFINITION_ERR,
                        "No index columns was specified."
                );
            } else if (params.collations().size() != params.columns().size()) {
                throw new IgniteInternalException(
                        ErrorGroups.Index.INVALID_INDEX_DEFINITION_ERR,
                        "Columns collations doesn't match number of columns."
                );
            }

            Predicate<String> duplicateValidator = Predicate.not(new HashSet<>()::add);

            for (String columnName : params.columns()) {
                CatalogTableColumnDescriptor columnDescriptor = table.columnDescriptor(columnName);

                if (columnDescriptor == null) {
                    throw new ColumnNotFoundException(columnName);
                } else if (duplicateValidator.test(columnName)) {
                    throw new IgniteInternalException(
                            ErrorGroups.Index.INVALID_INDEX_DEFINITION_ERR,
                            "Can't create index on duplicate columns: " + String.join(", ", params.columns())
                    );
                }
            }

            CatalogIndexDescriptor index = CatalogUtils.fromParams(catalog.objectIdGenState(), table.id(), params);

            return List.of(
                    new NewIndexEntry(index),
                    new ObjectIdGenUpdateEntry(1)
            );
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> dropIndex(DropIndexParams params) {
        return saveUpdate(catalog -> {
            String schemaName = Objects.requireNonNullElse(params.schemaName(), CatalogService.PUBLIC);

            CatalogSchemaDescriptor schema = Objects.requireNonNull(catalog.schema(schemaName), "No schema found: " + schemaName);

            CatalogIndexDescriptor index = schema.index(params.indexName());

            if (index == null) {
                throw new IndexNotFoundException(schemaName, params.indexName());
            }

            return List.of(
                    new DropIndexEntry(index.id())
            );
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> createDistributionZone(CreateZoneParams params) {
        if (params.dataNodesAutoAdjust() != INFINITE_TIMER_VALUE
                && (params.dataNodesAutoAdjustScaleUp() != INFINITE_TIMER_VALUE
                || params.dataNodesAutoAdjustScaleDown() != INFINITE_TIMER_VALUE)
        ) {
            return failedFuture(new IgniteInternalException(
                    DistributionZones.ZONE_DEFINITION_ERR,
                    "Not compatible parameters [dataNodesAutoAdjust=" + params.dataNodesAutoAdjust()
                            + ", dataNodesAutoAdjustScaleUp=" + params.dataNodesAutoAdjustScaleUp()
                            + ", dataNodesAutoAdjustScaleDown=" + params.dataNodesAutoAdjustScaleDown() + ']'
            ));
        }

        return saveUpdate(catalog -> {
            String zoneName = Objects.requireNonNull(params.zoneName(), "zone");

            if (catalog.zone(params.zoneName()) != null) {
                throw new DistributionZoneAlreadyExistsException(zoneName);
            }

            CatalogZoneDescriptor zone = CatalogUtils.fromParams(catalog.objectIdGenState(), params);

            return List.of(
                    new NewZoneEntry(zone),
                    new ObjectIdGenUpdateEntry(1)
            );
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> dropDistributionZone(DropZoneParams params) {
        return saveUpdate(catalog -> {
            String zoneName = Objects.requireNonNull(params.zoneName(), "zone");

            CatalogZoneDescriptor zone = catalog.zone(zoneName);

            if (zone == null) {
                throw new DistributionZoneNotFoundException(zoneName);
            }
            if (zone.name().equals(CatalogService.DEFAULT_ZONE_NAME)) {
                //TODO IGNITE-19082 Can default zone be dropped?
                throw new IgniteInternalException(
                        DistributionZones.ZONE_DROP_ERR,
                        "Default distribution zone can't be dropped"
                );
            }

            catalog.schemas().stream()
                    .flatMap(s -> Arrays.stream(s.tables()))
                    .filter(t -> t.zoneId() == zone.id())
                    .findAny()
                    .ifPresent(t -> {
                        throw new DistributionZoneBindTableException(zoneName, t.name());
                    });

            return List.of(
                    new DropZoneEntry(zone.id())
            );
        });
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Void> renameDistributionZone(RenameZoneParams params) {
        return saveUpdate(catalog -> {
            String zoneName = Objects.requireNonNull(params.newZoneName(), "newZoneName");

            CatalogZoneDescriptor zone = catalog.zone(params.zoneName());

            if (zone == null) {
                throw new DistributionZoneNotFoundException(zoneName);
            }
            if (catalog.zone(params.newZoneName()) != null) {
                throw new DistributionZoneAlreadyExistsException(params.newZoneName());
            }
            if (zone.name().equals(CatalogService.DEFAULT_ZONE_NAME)) {
                //TODO IGNITE-19082 Can default zone be renamed?
                throw new IgniteInternalException(
                        DistributionZones.ZONE_RENAME_ERR,
                        "Default distribution zone can't be renamed"
                );
            }

            CatalogZoneDescriptor descriptor = new CatalogZoneDescriptor(
                    zone.id(),
                    params.newZoneName(),
                    zone.partitions(),
                    zone.replicas(),
                    zone.dataNodesAutoAdjust(),
                    zone.dataNodesAutoAdjustScaleUp(),
                    zone.dataNodesAutoAdjustScaleDown(),
                    zone.filter()
            );

            return List.of(new AlterZoneEntry(descriptor));
        });
    }

    @Override
    public CompletableFuture<Void> alterDistributionZone(AlterZoneParams params) {
        return saveUpdate(catalog -> {
            CatalogZoneDescriptor zone = catalog.zone(params.zoneName());

            if (zone == null) {
                throw new DistributionZoneNotFoundException(params.zoneName());
            }

            Integer dataNodesAutoAdjust = params.dataNodesAutoAdjust();
            Integer dataNodesAutoAdjustScaleUp = params.dataNodesAutoAdjustScaleUp();
            Integer dataNodesAutoAdjustScaleDown = params.dataNodesAutoAdjustScaleDown();

            if (dataNodesAutoAdjust == null && dataNodesAutoAdjustScaleUp == null && dataNodesAutoAdjustScaleDown == null) {
                dataNodesAutoAdjust = zone.dataNodesAutoAdjust();
                dataNodesAutoAdjustScaleUp = zone.dataNodesAutoAdjustScaleUp();
                dataNodesAutoAdjustScaleDown = zone.dataNodesAutoAdjustScaleDown();
            } else {
                if (dataNodesAutoAdjust != null && (dataNodesAutoAdjustScaleUp != null || dataNodesAutoAdjustScaleDown != null)) {
                    throw new IgniteInternalException(
                            DistributionZones.ZONE_DEFINITION_ERR,
                            "Not compatible parameters [dataNodesAutoAdjust=" + params.dataNodesAutoAdjust()
                                    + ", dataNodesAutoAdjustScaleUp=" + params.dataNodesAutoAdjustScaleUp()
                                    + ", dataNodesAutoAdjustScaleDown=" + params.dataNodesAutoAdjustScaleDown() + ']'
                    );
                }

                dataNodesAutoAdjust = Objects.requireNonNullElse(params.dataNodesAutoAdjust(), INFINITE_TIMER_VALUE);
                dataNodesAutoAdjustScaleUp = Objects.requireNonNullElse(params.dataNodesAutoAdjustScaleUp(), INFINITE_TIMER_VALUE);
                dataNodesAutoAdjustScaleDown = Objects.requireNonNullElse(params.dataNodesAutoAdjustScaleDown(), INFINITE_TIMER_VALUE);
            }

            CatalogZoneDescriptor descriptor = new CatalogZoneDescriptor(
                    zone.id(),
                    zone.name(),
                    Objects.requireNonNullElse(params.partitions(), zone.partitions()),
                    Objects.requireNonNullElse(params.replicas(), zone.replicas()),
                    dataNodesAutoAdjust,
                    dataNodesAutoAdjustScaleUp,
                    dataNodesAutoAdjustScaleDown,
                    Objects.requireNonNullElse(params.filter(), zone.filter())
            );

            return List.of(new AlterZoneEntry(descriptor));
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

        return updateLog.append(new VersionedUpdate(newVersion, delayDurationMs, updates))
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
        public void handle(VersionedUpdate update, HybridTimestamp metastoreTimestamp) {
            int version = update.version();
            long activationTimestamp = metastoreTimestamp.addPhysicalTime(update.delayDuration()).longValue();
            Catalog catalog = catalogByVer.get(version - 1);

            assert catalog != null;

            List<CompletableFuture<?>> eventFutures = new ArrayList<>(update.entries().size());

            for (UpdateEntry entry : update.entries()) {
                String schemaName = CatalogService.PUBLIC;
                CatalogSchemaDescriptor schema = Objects.requireNonNull(catalog.schema(schemaName), "No schema found: " + schemaName);

                if (entry instanceof NewTableEntry) {
                    catalog = new Catalog(
                            version,
                            activationTimestamp,
                            catalog.objectIdGenState(),
                            catalog.zones(),
                            List.of(new CatalogSchemaDescriptor(
                                    schema.id(),
                                    schema.name(),
                                    ArrayUtils.concat(schema.tables(), ((NewTableEntry) entry).descriptor()),
                                    schema.indexes()
                            ))
                    );

                    eventFutures.add(fireEvent(
                            CatalogEvent.TABLE_CREATE,
                            new CreateTableEventParameters(version, ((NewTableEntry) entry).descriptor())
                    ));

                } else if (entry instanceof DropTableEntry) {
                    int tableId = ((DropTableEntry) entry).tableId();

                    catalog = new Catalog(
                            version,
                            activationTimestamp,
                            catalog.objectIdGenState(),
                            catalog.zones(),
                            List.of(new CatalogSchemaDescriptor(
                                    schema.id(),
                                    schema.name(),
                                    Arrays.stream(schema.tables()).filter(t -> t.id() != tableId).toArray(CatalogTableDescriptor[]::new),
                                    schema.indexes()
                            ))
                    );

                    eventFutures.add(fireEvent(
                            CatalogEvent.TABLE_DROP,
                            new DropTableEventParameters(version, tableId)
                    ));
                } else if (entry instanceof NewColumnsEntry) {
                    int tableId = ((NewColumnsEntry) entry).tableId();
                    List<CatalogTableColumnDescriptor> columnDescriptors = ((NewColumnsEntry) entry).descriptors();

                    catalog = new Catalog(
                            version,
                            activationTimestamp,
                            catalog.objectIdGenState(),
                            catalog.zones(),
                            List.of(new CatalogSchemaDescriptor(
                                    schema.id(),
                                    schema.name(),
                                    Arrays.stream(schema.tables())
                                            .map(table -> table.id() != tableId
                                                    ? table
                                                    : new CatalogTableDescriptor(
                                                            table.id(),
                                                            table.name(),
                                                            table.zoneId(),
                                                            CollectionUtils.concat(table.columns(), columnDescriptors),
                                                            table.primaryKeyColumns(),
                                                            table.colocationColumns())
                                            )
                                            .toArray(CatalogTableDescriptor[]::new),
                                    schema.indexes()
                            ))
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
                            activationTimestamp,
                            catalog.objectIdGenState(),
                            catalog.zones(),
                            List.of(new CatalogSchemaDescriptor(
                                    schema.id(),
                                    schema.name(),
                                    Arrays.stream(schema.tables())
                                            .map(table -> table.id() != tableId
                                                    ? table
                                                    : new CatalogTableDescriptor(
                                                            table.id(),
                                                            table.name(),
                                                            table.zoneId(),
                                                            table.columns().stream()
                                                                    .filter(col -> !columns.contains(col.name()))
                                                                    .collect(toList()),
                                                            table.primaryKeyColumns(),
                                                            table.colocationColumns())
                                            )
                                            .toArray(CatalogTableDescriptor[]::new),
                                    schema.indexes()
                            ))
                    );

                    eventFutures.add(fireEvent(
                            CatalogEvent.TABLE_ALTER,
                            new DropColumnEventParameters(version, tableId, columns)
                    ));
                } else if (entry instanceof NewIndexEntry) {
                    catalog = new Catalog(
                            version,
                            activationTimestamp,
                            catalog.objectIdGenState(),
                            catalog.zones(),
                            List.of(new CatalogSchemaDescriptor(
                                    schema.id(),
                                    schema.name(),
                                    schema.tables(),
                                    ArrayUtils.concat(schema.indexes(), ((NewIndexEntry) entry).descriptor())
                            ))
                    );

                    eventFutures.add(fireEvent(
                            CatalogEvent.INDEX_CREATE,
                            new CreateIndexEventParameters(version, ((NewIndexEntry) entry).descriptor())
                    ));
                } else if (entry instanceof DropIndexEntry) {
                    int indexId = ((DropIndexEntry) entry).indexId();

                    catalog = new Catalog(
                            version,
                            activationTimestamp,
                            catalog.objectIdGenState(),
                            catalog.zones(),
                            List.of(new CatalogSchemaDescriptor(
                                    schema.id(),
                                    schema.name(),
                                    schema.tables(),
                                    Arrays.stream(schema.indexes()).filter(t -> t.id() != indexId).toArray(CatalogIndexDescriptor[]::new)
                            ))
                    );

                    eventFutures.add(fireEvent(
                            CatalogEvent.INDEX_DROP,
                            new DropIndexEventParameters(version, indexId)
                    ));
                } else if (entry instanceof NewZoneEntry) {
                    catalog = new Catalog(
                            version,
                            activationTimestamp,
                            catalog.objectIdGenState(),
                            CollectionUtils.concat(catalog.zones(), List.of(((NewZoneEntry) entry).descriptor())),
                            catalog.schemas()
                    );

                    eventFutures.add(fireEvent(
                            CatalogEvent.ZONE_CREATE,
                            new CreateZoneEventParameters(version, ((NewZoneEntry) entry).descriptor())
                    ));
                } else if (entry instanceof DropZoneEntry) {
                    int zoneId = ((DropZoneEntry) entry).zoneId();

                    catalog = new Catalog(
                            version,
                            activationTimestamp,
                            catalog.objectIdGenState(),
                            catalog.zones().stream().filter(z -> z.id() != zoneId).collect(toList()),
                            catalog.schemas()
                    );

                    eventFutures.add(fireEvent(
                            CatalogEvent.ZONE_DROP,
                            new DropZoneEventParameters(version, zoneId)
                    ));
                } else if (entry instanceof AlterZoneEntry) {
                    CatalogZoneDescriptor descriptor = ((AlterZoneEntry) entry).descriptor();

                    catalog = new Catalog(
                            version,
                            activationTimestamp,
                            catalog.objectIdGenState(),
                            catalog.zones().stream()
                                    .map(z -> z.id() == descriptor.id() ? descriptor : z)
                                    .collect(toList()),
                            catalog.schemas()
                    );

                    eventFutures.add(fireEvent(
                            CatalogEvent.ZONE_ALTER,
                            new AlterZoneEventParameters(version, descriptor)
                    ));
                } else if (entry instanceof ObjectIdGenUpdateEntry) {
                    catalog = new Catalog(
                            version,
                            activationTimestamp,
                            catalog.objectIdGenState() + ((ObjectIdGenUpdateEntry) entry).delta(),
                            catalog.zones(),
                            catalog.schemas()
                    );
                } else if (entry instanceof AlterColumnEntry) {
                    int tableId = ((AlterColumnEntry) entry).tableId();
                    CatalogTableColumnDescriptor target = ((AlterColumnEntry) entry).descriptor();

                    catalog = new Catalog(
                            version,
                            activationTimestamp,
                            catalog.objectIdGenState(),
                            catalog.zones(),
                            List.of(new CatalogSchemaDescriptor(
                                    schema.id(),
                                    schema.name(),
                                    Arrays.stream(schema.tables())
                                            .map(table -> table.id() != tableId
                                                    ? table
                                                    : new CatalogTableDescriptor(
                                                            table.id(),
                                                            table.name(),
                                                            table.zoneId(),
                                                            table.columns().stream()
                                                                    .map(source -> source.name().equals(target.name()) ? target : source)
                                                                    .collect(toList()),
                                                            table.primaryKeyColumns(),
                                                            table.colocationColumns())
                                            )
                                            .toArray(CatalogTableDescriptor[]::new),
                                    schema.indexes()
                            ))
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
                    .whenComplete((ignore, err) -> {
                        if (err != null) {
                            LOG.warn("Failed to apply catalog update.", err);
                            //TODO: IGNITE-14611 Pass exception to an error handler because catalog got into inconsistent state.
                        }

                        versionTracker.update(version, null);
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
