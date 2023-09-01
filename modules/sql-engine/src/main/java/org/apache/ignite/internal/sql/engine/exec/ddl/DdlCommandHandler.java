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

package org.apache.ignite.internal.sql.engine.exec.ddl;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.sql.engine.SqlQueryProcessor.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.lang.ErrorGroups.Sql.STMT_VALIDATION_ERR;

import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.internal.distributionzones.DistributionZoneAlreadyExistsException;
import org.apache.ignite.internal.distributionzones.DistributionZoneConfigurationParameters;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.DistributionZoneNotFoundException;
import org.apache.ignite.internal.index.IndexManager;
import org.apache.ignite.internal.schema.BitmaskNativeType;
import org.apache.ignite.internal.schema.DecimalNativeType;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.NumberNativeType;
import org.apache.ignite.internal.schema.TemporalNativeType;
import org.apache.ignite.internal.schema.VarlenNativeType;
import org.apache.ignite.internal.schema.configuration.ColumnChange;
import org.apache.ignite.internal.schema.configuration.ColumnTypeChange;
import org.apache.ignite.internal.schema.configuration.ColumnView;
import org.apache.ignite.internal.schema.configuration.PrimaryKeyView;
import org.apache.ignite.internal.schema.configuration.TableChange;
import org.apache.ignite.internal.schema.configuration.ValueSerializationHelper;
import org.apache.ignite.internal.schema.configuration.defaultvalue.ConstantValueDefaultChange;
import org.apache.ignite.internal.schema.configuration.defaultvalue.FunctionCallDefaultChange;
import org.apache.ignite.internal.schema.configuration.defaultvalue.NullValueDefaultChange;
import org.apache.ignite.internal.schema.configuration.index.HashIndexChange;
import org.apache.ignite.internal.schema.configuration.index.HashIndexView;
import org.apache.ignite.internal.schema.configuration.index.IndexColumnView;
import org.apache.ignite.internal.schema.configuration.index.SortedIndexChange;
import org.apache.ignite.internal.schema.configuration.index.SortedIndexView;
import org.apache.ignite.internal.schema.configuration.index.TableIndexChange;
import org.apache.ignite.internal.schema.configuration.index.TableIndexView;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AbstractTableDdlCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterColumnCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterTableAddCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterTableDropCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterZoneRenameCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterZoneSetCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.ColumnDefinition;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateIndexCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateTableCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateZoneCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DdlCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DefaultValueDefinition.ConstantValue;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DefaultValueDefinition.FunctionCall;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DropIndexCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DropTableCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DropZoneCommand;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Collation;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.lang.ColumnAlreadyExistsException;
import org.apache.ignite.lang.ColumnNotFoundException;
import org.apache.ignite.lang.ErrorGroups;
import org.apache.ignite.lang.ErrorGroups.Table;
import org.apache.ignite.lang.IgniteStringBuilder;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.lang.util.StringUtils;
import org.apache.ignite.sql.SqlException;

/** DDL commands handler. */
public class DdlCommandHandler {
    private final DistributionZoneManager distributionZoneManager;

    private final TableManager tableManager;

    private final IndexManager indexManager;

    private final DataStorageManager dataStorageManager;

    /**
     * Constructor.
     */
    public DdlCommandHandler(
            DistributionZoneManager distributionZoneManager,
            TableManager tableManager,
            IndexManager indexManager,
            DataStorageManager dataStorageManager
    ) {
        this.distributionZoneManager = distributionZoneManager;
        this.tableManager = tableManager;
        this.indexManager = indexManager;
        this.dataStorageManager = dataStorageManager;
    }

    /** Handles ddl commands. */
    @WithSpan
    public CompletableFuture<Boolean> handle(DdlCommand cmd) {
        validateCommand(cmd);

        if (cmd instanceof CreateTableCommand) {
            return handleCreateTable((CreateTableCommand) cmd);
        } else if (cmd instanceof DropTableCommand) {
            return handleDropTable((DropTableCommand) cmd);
        } else if (cmd instanceof AlterTableAddCommand) {
            return handleAlterAddColumn((AlterTableAddCommand) cmd);
        } else if (cmd instanceof AlterTableDropCommand) {
            return handleAlterDropColumn((AlterTableDropCommand) cmd);
        } else if (cmd instanceof AlterColumnCommand) {
            return completedFuture(true);
        } else if (cmd instanceof CreateIndexCommand) {
            return handleCreateIndex((CreateIndexCommand) cmd);
        } else if (cmd instanceof DropIndexCommand) {
            return handleDropIndex((DropIndexCommand) cmd);
        } else if (cmd instanceof CreateZoneCommand) {
            return handleCreateZone((CreateZoneCommand) cmd);
        } else if (cmd instanceof AlterZoneRenameCommand) {
            return handleRenameZone((AlterZoneRenameCommand) cmd);
        } else if (cmd instanceof AlterZoneSetCommand) {
            return handleAlterZone((AlterZoneSetCommand) cmd);
        } else if (cmd instanceof DropZoneCommand) {
            return handleDropZone((DropZoneCommand) cmd);
        } else {
            return failedFuture(new SqlException(STMT_VALIDATION_ERR, "Unsupported DDL operation ["
                    + "cmdName=" + (cmd == null ? null : cmd.getClass().getSimpleName()) + "; "
                    + "cmd=\"" + cmd + "\"]"));
        }
    }

    /** Validate command. */
    private void validateCommand(DdlCommand cmd) {
        if (cmd instanceof AbstractTableDdlCommand) {
            AbstractTableDdlCommand cmd0 = (AbstractTableDdlCommand) cmd;

            if (StringUtils.nullOrEmpty(cmd0.tableName())) {
                throw new IllegalArgumentException("Table name is undefined.");
            }
        }
    }

    /** Handles create distribution zone command. */
    @WithSpan
    private CompletableFuture<Boolean> handleCreateZone(CreateZoneCommand cmd) {
        DistributionZoneConfigurationParameters.Builder zoneCfgBuilder =
                new DistributionZoneConfigurationParameters.Builder(cmd.zoneName());

        if (cmd.dataNodesAutoAdjust() != null) {
            zoneCfgBuilder.dataNodesAutoAdjust(cmd.dataNodesAutoAdjust());
        }

        if (cmd.dataNodesAutoAdjustScaleUp() != null) {
            zoneCfgBuilder.dataNodesAutoAdjustScaleUp(cmd.dataNodesAutoAdjustScaleUp());
        }

        if (cmd.dataNodesAutoAdjustScaleDown() != null) {
            zoneCfgBuilder.dataNodesAutoAdjustScaleDown(cmd.dataNodesAutoAdjustScaleDown());
        }

        if (cmd.replicas() != null) {
            zoneCfgBuilder.replicas(cmd.replicas());
        }

        if (cmd.partitions() != null) {
            zoneCfgBuilder.partitions(cmd.partitions());
        }

        if (cmd.nodeFilter() != null) {
            zoneCfgBuilder.filter(cmd.nodeFilter());
        }

        zoneCfgBuilder.dataStorageChangeConsumer(
                dataStorageManager.zoneDataStorageConsumer(cmd.dataStorage(), cmd.dataStorageOptions()));

        return distributionZoneManager.createZone(zoneCfgBuilder.build())
                .handle(handleModificationResult(cmd.ifNotExists(), DistributionZoneAlreadyExistsException.class));
    }


    /** Handles rename zone command. */
    @WithSpan
    private CompletableFuture<Boolean> handleRenameZone(AlterZoneRenameCommand cmd) {
        DistributionZoneConfigurationParameters.Builder zoneCfgBuilder =
                new DistributionZoneConfigurationParameters.Builder(cmd.newZoneName());

        boolean ifExists = cmd.ifExists();

        return distributionZoneManager.alterZone(cmd.zoneName(), zoneCfgBuilder.build())
                .handle(handleModificationResult(ifExists, DistributionZoneNotFoundException.class));
    }

    /** Handles alter zone command. */
    @WithSpan
    private CompletableFuture<Boolean> handleAlterZone(AlterZoneSetCommand cmd) {
        DistributionZoneConfigurationParameters.Builder zoneCfgBuilder =
                new DistributionZoneConfigurationParameters.Builder(cmd.zoneName());

        if (cmd.dataNodesAutoAdjustScaleDown() != null) {
            zoneCfgBuilder.dataNodesAutoAdjustScaleDown(cmd.dataNodesAutoAdjustScaleDown());
        }

        if (cmd.dataNodesAutoAdjust() != null) {
            zoneCfgBuilder.dataNodesAutoAdjust(cmd.dataNodesAutoAdjust());
        }

        if (cmd.dataNodesAutoAdjustScaleUp() != null) {
            zoneCfgBuilder.dataNodesAutoAdjustScaleUp(cmd.dataNodesAutoAdjustScaleUp());
        }

        if (cmd.replicas() != null) {
            zoneCfgBuilder.replicas(cmd.replicas());
        }

        if (cmd.partitions() != null) {
            zoneCfgBuilder.partitions(cmd.partitions());
        }

        if (cmd.nodeFilter() != null) {
            zoneCfgBuilder.filter(cmd.nodeFilter());
        }

        return distributionZoneManager.alterZone(cmd.zoneName(), zoneCfgBuilder.build())
                .handle(handleModificationResult(cmd.ifExists(), DistributionZoneNotFoundException.class));
    }

    /** Handles drop distribution zone command. */
    @WithSpan
    private CompletableFuture<Boolean> handleDropZone(DropZoneCommand cmd) {
        return distributionZoneManager.dropZone(cmd.zoneName())
                .handle(handleModificationResult(cmd.ifExists(), DistributionZoneNotFoundException.class));
    }

    /** Handles create table command. */
    @WithSpan
    private CompletableFuture<Boolean> handleCreateTable(CreateTableCommand cmd) {
        cmd.columns().stream()
                .map(ColumnDefinition::name)
                .filter(Predicate.not(new HashSet<>()::add))
                .findAny()
                .ifPresent(col -> {
                    throw new SqlException(Table.TABLE_DEFINITION_ERR, "Can't create table with duplicate columns: "
                            + cmd.columns().stream().map(ColumnDefinition::name).collect(Collectors.joining(", ")));
                });

        Consumer<TableChange> tblChanger = tableChange -> {
            tableChange.changeColumns(columnsChange -> {
                for (var col : cmd.columns()) {
                    columnsChange.create(col.name(), columnChange -> convertColumnDefinition(col, columnChange));
                }
            });

            var colocationKeys = cmd.colocationColumns();

            if (nullOrEmpty(colocationKeys)) {
                colocationKeys = cmd.primaryKeyColumns();
            }

            var colocationKeys0 = colocationKeys;

            tableChange.changePrimaryKey(pkChange -> pkChange.changeColumns(cmd.primaryKeyColumns().toArray(String[]::new))
                    .changeColocationColumns(colocationKeys0.toArray(String[]::new)));
        };

        String zoneName;

        if (cmd.zone() != null) {
            zoneName = cmd.zone();
        } else {
            zoneName = DEFAULT_ZONE_NAME;
        }

        return tableManager.createTableAsync(cmd.tableName(), zoneName, tblChanger)
                .thenApply(Objects::nonNull)
                .handle(handleModificationResult(cmd.ifTableExists(), TableAlreadyExistsException.class));
    }

    /** Handles drop table command. */
    @WithSpan
    private CompletableFuture<Boolean> handleDropTable(DropTableCommand cmd) {
        return tableManager.dropTableAsync(cmd.tableName())
                .thenApply(v -> Boolean.TRUE)
                .handle(handleModificationResult(cmd.ifTableExists(), TableNotFoundException.class));
    }

    /** Handles add column command. */
    @WithSpan
    private CompletableFuture<Boolean> handleAlterAddColumn(AlterTableAddCommand cmd) {
        if (nullOrEmpty(cmd.columns())) {
            return completedFuture(Boolean.FALSE);
        }

        return addColumnInternal(cmd.tableName(), cmd.columns())
                .handle(handleModificationResult(cmd.ifTableExists(), TableNotFoundException.class));
    }

    /** Handles drop column command. */
    @WithSpan
    private CompletableFuture<Boolean> handleAlterDropColumn(AlterTableDropCommand cmd) {
        if (nullOrEmpty(cmd.columns())) {
            return completedFuture(Boolean.FALSE);
        }

        return dropColumnInternal(cmd.tableName(), cmd.columns())
                .handle(handleModificationResult(cmd.ifTableExists(), TableNotFoundException.class));
    }

    protected static BiFunction<Object, Throwable, Boolean> handleModificationResult(boolean ignoreExpectedError, Class<?> expErrCls) {
        return (val, err) -> {
            if (err == null) {
                return val instanceof Boolean ? (Boolean) val : Boolean.TRUE;
            } else if (ignoreExpectedError) {
                Throwable err0 = err instanceof CompletionException ? err.getCause() : err;

                if (expErrCls.isAssignableFrom(err0.getClass())) {
                    return Boolean.FALSE;
                }
            }

            throw (err instanceof RuntimeException) ? (RuntimeException) err : new CompletionException(err);
        };
    }

    /** Handles create index command. */
    @WithSpan
    private CompletableFuture<Boolean> handleCreateIndex(CreateIndexCommand cmd) {
        cmd.columns().stream()
                .filter(Predicate.not(new HashSet<>()::add))
                .findAny()
                .ifPresent(col -> {
                    throw new SqlException(ErrorGroups.Index.INVALID_INDEX_DEFINITION_ERR,
                            "Can't create index on duplicate columns: " + String.join(", ", cmd.columns()));
                });

        Consumer<TableIndexChange> indexChanger = tableIndexChange -> {
            switch (cmd.type()) {
                case SORTED:
                    createSortedIndexInternal(cmd, tableIndexChange.convert(SortedIndexChange.class));

                    break;
                case HASH:
                    createHashIndexInternal(cmd, tableIndexChange.convert(HashIndexChange.class));

                    break;
                default:
                    throw new AssertionError("Unknown index type [type=" + cmd.type() + "]");
            }
        };

        return indexManager.createIndexAsync(
                cmd.schemaName(),
                cmd.indexName(),
                cmd.tableName(),
                !cmd.ifNotExists(),
                indexChanger);
    }

    /** Handles drop index command. */
    @WithSpan
    private CompletableFuture<Boolean> handleDropIndex(DropIndexCommand cmd) {
        return indexManager.dropIndexAsync(cmd.schemaName(), cmd.indexName(), !cmd.ifNotExists());
    }

    /**
     * Creates sorted index.
     *
     * @param cmd Create index command.
     * @param indexChange Index configuration changer.
     */
    private void createSortedIndexInternal(CreateIndexCommand cmd, SortedIndexChange indexChange) {
        indexChange.changeColumns(colsInit -> {
            for (int i = 0; i < cmd.columns().size(); i++) {
                String columnName = cmd.columns().get(i);
                Collation collation = cmd.collations().get(i);
                //TODO: https://issues.apache.org/jira/browse/IGNITE-17563 Pass null ordering for columns.
                colsInit.create(columnName, colInit -> colInit.changeAsc(collation.asc));
            }
        });
    }

    /**
     * Creates hash index.
     *
     * @param cmd Create index command.
     * @param indexChange Index configuration changer.
     */
    private void createHashIndexInternal(CreateIndexCommand cmd, HashIndexChange indexChange) {
        indexChange.changeColumnNames(cmd.columns().toArray(ArrayUtils.STRING_EMPTY_ARRAY));
    }

    /**
     * Adds a column according to the column definition.
     *
     * @param fullName Table with schema name.
     * @param colsDef Columns defenitions.
     * @return {@code true} if the full columns set is applied successfully. Otherwise, returns {@code false}.
     */
    private CompletableFuture<Boolean> addColumnInternal(String fullName, List<ColumnDefinition> colsDef) {
        AtomicBoolean retUsr = new AtomicBoolean(true);

        return tableManager.alterTableAsync(
                fullName,
                chng -> {
                    AtomicBoolean retTbl = new AtomicBoolean();

                    chng.changeColumns(cols -> {
                        retUsr.set(true); // Reset state if closure have been restarted.

                        Set<String> colNamesToOrders = columnNames(chng.columns());

                        colsDef.stream()
                                .filter(k -> colNamesToOrders.contains(k.name()))
                                .findAny()
                                .ifPresent(c -> {
                                    throw new ColumnAlreadyExistsException(c.name());
                                });

                        for (ColumnDefinition col : colsDef) {
                            cols.create(col.name(), colChg -> convertColumnDefinition(col, colChg));
                        }

                        retTbl.set(!colsDef.isEmpty());
                    });

                    return retTbl.get();
                }
        ).thenApply(v -> retUsr.get());
    }

    private void convertColumnDefinition(ColumnDefinition definition, ColumnChange columnChange) {
        NativeType columnType = IgniteTypeFactory.relDataTypeToNative(definition.type());

        columnChange.changeType(columnTypeChange -> convert(columnType, columnTypeChange));
        columnChange.changeNullable(definition.nullable());
        columnChange.changeDefaultValueProvider(defaultChange -> {
            switch (definition.defaultValueDefinition().type()) {
                case CONSTANT:
                    ConstantValue constantValue = definition.defaultValueDefinition();

                    var val = constantValue.value();

                    if (val != null) {
                        defaultChange.convert(ConstantValueDefaultChange.class)
                                .changeDefaultValue(ValueSerializationHelper.toString(val, columnType));
                    } else {
                        defaultChange.convert(NullValueDefaultChange.class);
                    }

                    break;
                case FUNCTION_CALL:
                    FunctionCall functionCall = definition.defaultValueDefinition();

                    defaultChange.convert(FunctionCallDefaultChange.class)
                            .changeFunctionName(functionCall.functionName());

                    break;
                default:
                    throw new IllegalStateException("Unknown default value definition type [type="
                            + definition.defaultValueDefinition().type() + ']');
            }
        });
    }

    /**
     * Drops a column(s) exceptional behavior depends on {@code colExist} flag.
     *
     * @param tableName Table name.
     * @param colNames Columns definitions.
     * @return {@code true} if the full columns set is applied successfully. Otherwise, returns {@code false}.
     */
    private CompletableFuture<Boolean> dropColumnInternal(String tableName, Set<String> colNames) {
        AtomicBoolean ret = new AtomicBoolean(true);

        return tableManager.alterTableAsync(
                tableName,
                chng -> {
                    chng.changeColumns(cols -> {
                        ret.set(true); // Reset state if closure have been restarted.

                        PrimaryKeyView priKey = chng.primaryKey();

                        Set<String> colNamesToOrders = columnNames(chng.columns());

                        Set<String> colNames0 = new HashSet<>();

                        Set<String> primaryCols = Set.of(priKey.columns());

                        reportIndexedColumns(tableName, colNames, primaryCols);

                        for (String colName : colNames) {
                            if (!colNamesToOrders.contains(colName)) {
                                ret.set(false);

                                throw new ColumnNotFoundException(DEFAULT_SCHEMA_NAME, tableName, colName);
                            } else {
                                colNames0.add(colName);
                            }

                            if (primaryCols.contains(colName)) {
                                throw new SqlException(STMT_VALIDATION_ERR, IgniteStringFormatter
                                        .format("Can`t delete column, belongs to primary key: [name={}]", colName));
                            }
                        }

                        colNames0.forEach(cols::delete);
                    });

                    return ret.get();
                }).thenApply(v -> ret.get());
    }

    private void reportIndexedColumns(String tableName, Set<String> colNames, Set<String> pkColNames) throws SqlException {
        Map<String, List<String>> indexedColumns = new HashMap<>();

        for (TableIndexView idxCfg : indexManager.indexConfigurations(tableName)) {
            if (idxCfg instanceof SortedIndexView) {
                for (IndexColumnView colView : ((SortedIndexView) idxCfg).columns()) {
                    if (colNames.contains(colView.name()) && !pkColNames.contains(colView.name())) {
                        indexedColumns.computeIfAbsent(colView.name(), v -> new ArrayList<>()).add(idxCfg.name());
                    }
                }
            } else if (idxCfg instanceof HashIndexView) {
                for (String colName : ((HashIndexView) idxCfg).columnNames()) {
                    if (colNames.contains(colName) && !pkColNames.contains(colName)) {
                        indexedColumns.computeIfAbsent(colName, v -> new ArrayList<>()).add(idxCfg.name());
                    }
                }
            }
        }

        if (indexedColumns.isEmpty()) {
            return;
        }

        IgniteStringBuilder sb = new IgniteStringBuilder("Can`t delete column(s). ");

        for (Entry<String, List<String>> e : indexedColumns.entrySet()) {
            sb.app("Column ").app(e.getKey()).app(" is used by indexes ").app(e.getValue()).app(". ");
        }

        throw new SqlException(STMT_VALIDATION_ERR, sb.toString());
    }

    private static void convert(NativeType colType, ColumnTypeChange colTypeChg) {
        NativeTypeSpec spec = colType.spec();
        String typeName = spec.name().toUpperCase();

        colTypeChg.changeType(typeName);

        switch (spec) {
            case BOOLEAN:
            case INT8:
            case INT16:
            case INT32:
            case INT64:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case UUID:
                // do nothing
                break;

            case BITMASK:
                BitmaskNativeType bitmaskColType = (BitmaskNativeType) colType;

                colTypeChg.changeLength(bitmaskColType.bits());

                break;

            case BYTES:
            case STRING:
                VarlenNativeType varLenColType = (VarlenNativeType) colType;

                colTypeChg.changeLength(varLenColType.length());

                break;

            case DECIMAL:
                DecimalNativeType numColType = (DecimalNativeType) colType;

                colTypeChg.changePrecision(numColType.precision());
                colTypeChg.changeScale(numColType.scale());

                break;

            case NUMBER:
                NumberNativeType numType = (NumberNativeType) colType;

                colTypeChg.changePrecision(numType.precision());

                break;

            case TIME:
            case DATETIME:
            case TIMESTAMP:
                TemporalNativeType temporalColType = (TemporalNativeType) colType;

                colTypeChg.changePrecision(temporalColType.precision());

                break;

            default:
                throw new IllegalArgumentException("Unknown type " + colType.spec().name());
        }
    }

    /** Column names set. */
    private static Set<String> columnNames(NamedListView<? extends ColumnView> cols) {
        return new HashSet<>(cols.namedListKeys());
    }
}
