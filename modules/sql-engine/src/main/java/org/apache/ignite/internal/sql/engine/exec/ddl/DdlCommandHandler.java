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

package org.apache.ignite.internal.sql.engine.exec.ddl;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter.convert;
import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter.convertDefaultToConfiguration;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.schemas.table.ColumnChange;
import org.apache.ignite.configuration.schemas.table.ColumnView;
import org.apache.ignite.configuration.schemas.table.ConstantValueDefaultChange;
import org.apache.ignite.configuration.schemas.table.FunctionCallDefaultChange;
import org.apache.ignite.configuration.schemas.table.HashIndexChange;
import org.apache.ignite.configuration.schemas.table.NullValueDefaultChange;
import org.apache.ignite.configuration.schemas.table.PrimaryKeyView;
import org.apache.ignite.configuration.schemas.table.SortedIndexChange;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.configuration.schemas.table.TableIndexChange;
import org.apache.ignite.internal.index.IndexManager;
import org.apache.ignite.internal.schema.SchemaUtils;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AbstractTableDdlCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterTableAddCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.AlterTableDropCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.ColumnDefinition;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateIndexCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.CreateTableCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DdlCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DefaultValueDefinition.ConstantValue;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DefaultValueDefinition.FunctionCall;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DropIndexCommand;
import org.apache.ignite.internal.sql.engine.prepare.ddl.DropTableCommand;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Collation;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.storage.DataStorageManager;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.util.ArrayUtils;
import org.apache.ignite.internal.util.IgniteObjectName;
import org.apache.ignite.internal.util.StringUtils;
import org.apache.ignite.lang.ColumnAlreadyExistsException;
import org.apache.ignite.lang.ColumnNotFoundException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.apache.ignite.lang.TableAlreadyExistsException;
import org.apache.ignite.lang.TableNotFoundException;

/** DDL commands handler. */
public class DdlCommandHandler {
    private final TableManager tableManager;

    private final IndexManager indexManager;

    private final DataStorageManager dataStorageManager;

    /**
     * Constructor.
     */
    public DdlCommandHandler(
            TableManager tableManager,
            IndexManager indexManager,
            DataStorageManager dataStorageManager
    ) {
        this.tableManager = tableManager;
        this.indexManager = indexManager;
        this.dataStorageManager = dataStorageManager;
    }

    /** Handles ddl commands. */
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
        } else if (cmd instanceof CreateIndexCommand) {
            return handleCreateIndex((CreateIndexCommand) cmd);
        } else if (cmd instanceof DropIndexCommand) {
            return handleDropIndex((DropIndexCommand) cmd);
        } else {
            return failedFuture(new IgniteInternalCheckedException("Unsupported DDL operation ["
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

    /** Handles create table command. */
    private CompletableFuture<Boolean> handleCreateTable(CreateTableCommand cmd) {
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

            tableChange.changeDataStorage(dataStorageManager.tableDataStorageConsumer(cmd.dataStorage(), cmd.dataStorageOptions()));

            if (cmd.partitions() != null) {
                tableChange.changePartitions(cmd.partitions());
            }

            if (cmd.replicas() != null) {
                tableChange.changeReplicas(cmd.replicas());
            }
        };

        String fullName = SchemaUtils.canonicalName(
                IgniteObjectName.quote(cmd.schemaName()),
                IgniteObjectName.quote(cmd.tableName())
        );

        return tableManager.createTableAsync(fullName, tblChanger)
                .thenApply(Objects::nonNull)
                .handle(handleTableModificationResult(cmd.ifTableExists()));
    }

    /** Handles drop table command. */
    private CompletableFuture<Boolean> handleDropTable(DropTableCommand cmd) {
        String fullName = SchemaUtils.canonicalName(
                IgniteObjectName.quote(cmd.schemaName()),
                IgniteObjectName.quote(cmd.tableName())
        );

        return tableManager.dropTableAsync(fullName)
                .thenApply(v -> Boolean.TRUE)
                .handle(handleTableModificationResult(cmd.ifTableExists()));
    }

    /** Handles add column command. */
    private CompletableFuture<Boolean> handleAlterAddColumn(AlterTableAddCommand cmd) {
        if (nullOrEmpty(cmd.columns())) {
            return completedFuture(Boolean.FALSE);
        }

        String fullName = SchemaUtils.canonicalName(
                IgniteObjectName.quote(cmd.schemaName()),
                IgniteObjectName.quote(cmd.tableName())
        );

        return addColumnInternal(fullName, cmd.columns(), cmd.ifColumnNotExists())
                .handle(handleTableModificationResult(cmd.ifTableExists()));
    }

    /** Handles drop column command. */
    private CompletableFuture<Boolean> handleAlterDropColumn(AlterTableDropCommand cmd) {
        if (nullOrEmpty(cmd.columns())) {
            return completedFuture(Boolean.FALSE);
        }

        String fullName = SchemaUtils.canonicalName(
                IgniteObjectName.quote(cmd.schemaName()),
                IgniteObjectName.quote(cmd.tableName())
        );

        return dropColumnInternal(fullName, cmd.columns(), cmd.ifColumnExists())
                .handle(handleTableModificationResult(cmd.ifTableExists()));
    }

    private static BiFunction<Object, Throwable, Boolean> handleTableModificationResult(boolean ignoreTableExistenceErrors) {
        return (val, err) -> {
            if (err == null) {
                return val instanceof Boolean ? (Boolean) val : Boolean.TRUE;
            } else if (ignoreTableExistenceErrors) {
                Throwable err0 = err instanceof CompletionException ? err.getCause() : err;

                if (err0 instanceof TableAlreadyExistsException || err0 instanceof TableNotFoundException) {
                    return Boolean.FALSE;
                }
            }

            throw (err instanceof RuntimeException) ? (RuntimeException) err : new CompletionException(err);
        };
    }

    /** Handles create index command. */
    private CompletableFuture<Boolean> handleCreateIndex(CreateIndexCommand cmd) {
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
                        indexChanger)
                .thenApply(Objects::nonNull);
    }

    /** Handles drop index command. */
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
     * @param ignoreColumnExistance Flag indicates exceptionally behavior in case of already existing column.
     * @return {@code true} if the full columns set is applied successfully. Otherwise, returns {@code false}.
     */
    private CompletableFuture<Boolean> addColumnInternal(String fullName, List<ColumnDefinition> colsDef, boolean ignoreColumnExistance) {
        AtomicBoolean ret = new AtomicBoolean(true);

        return tableManager.alterTableAsync(
                fullName,
                chng -> chng.changeColumns(cols -> {
                    Map<String, String> colNamesToOrders = columnOrdersToNames(chng.columns());

                    List<ColumnDefinition> colsDef0;

                    if (ignoreColumnExistance) {
                        colsDef0 = colsDef.stream().filter(k -> {
                            if (colNamesToOrders.containsKey(k.name())) {
                                ret.set(false);

                                return false;
                            } else {
                                return true;
                            }
                        }).collect(Collectors.toList());
                    } else {
                        colsDef.stream()
                                .filter(k -> colNamesToOrders.containsKey(k.name()))
                                .findAny()
                                .ifPresent(c -> {
                                    throw new ColumnAlreadyExistsException(c.name());
                                });

                        colsDef0 = colsDef;
                    }

                    for (ColumnDefinition col : colsDef0) {
                        cols.create(col.name(), colChg -> convertColumnDefinition(col, colChg));
                    }
                })
        ).thenApply(v -> ret.get());
    }

    private void convertColumnDefinition(ColumnDefinition definition, ColumnChange columnChange) {
        var columnType = IgniteTypeFactory.relDataTypeToColumnType(definition.type());

        columnChange.changeType(columnTypeChange -> convert(columnType, columnTypeChange));
        columnChange.changeNullable(definition.nullable());
        columnChange.changeDefaultValueProvider(defaultChange -> {
            switch (definition.defaultValueDefinition().type()) {
                case CONSTANT:
                    ConstantValue constantValue = definition.defaultValueDefinition();

                    var val = constantValue.value();

                    if (val != null) {
                        defaultChange.convert(ConstantValueDefaultChange.class)
                                .changeDefaultValue(convertDefaultToConfiguration(val, columnType));
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
     * @param fullName Table with schema name.
     * @param colNames Columns definitions.
     * @param ignoreColumnExistance Flag indicates exceptionally behavior in case of already existing column.
     * @return {@code true} if the full columns set is applied successfully. Otherwise, returns {@code false}.
     */
    private CompletableFuture<Boolean> dropColumnInternal(String fullName, Set<String> colNames, boolean ignoreColumnExistance) {
        AtomicBoolean ret = new AtomicBoolean(true);

        return tableManager.alterTableAsync(
                        fullName,
                        chng -> chng.changeColumns(cols -> {
                            PrimaryKeyView priKey = chng.primaryKey();

                            Map<String, String> colNamesToOrders = columnOrdersToNames(chng.columns());

                            Set<String> colNames0 = new HashSet<>();

                            Set<String> primaryCols = Set.of(priKey.columns());

                            for (String colName : colNames) {
                                if (!colNamesToOrders.containsKey(colName)) {
                                    ret.set(false);

                                    if (!ignoreColumnExistance) {
                                        throw new ColumnNotFoundException(colName, fullName);
                                    }
                                } else {
                                    colNames0.add(colName);
                                }

                                if (primaryCols.contains(colName)) {
                                    throw new IgniteException(IgniteStringFormatter
                                            .format("Can`t delete column, belongs to primary key: [name={}]", colName));
                                }
                            }

                            colNames0.forEach(k -> cols.delete(colNamesToOrders.get(k)));
                        }))
                .thenApply(v -> ret.get());
    }

    /** Map column name to order. */
    private static Map<String, String> columnOrdersToNames(NamedListView<? extends ColumnView> cols) {
        Map<String, String> colNames = new HashMap<>(cols.size());

        for (String colOrder : cols.namedListKeys()) {
            colNames.put(cols.get(colOrder).name(), colOrder);
        }

        return colNames;
    }
}
