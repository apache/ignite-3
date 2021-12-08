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

package org.apache.ignite.internal.processors.query.calcite.exec.ddl;

import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter.convert;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.schemas.table.ColumnView;
import org.apache.ignite.configuration.schemas.table.PrimaryKeyView;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.AbstractDdlCommand;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.AlterTableAddCommand;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.AlterTableDropCommand;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.CreateIndexCommand;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.CreateTableCommand;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.DdlCommand;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.DropIndexCommand;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.DropTableCommand;
import org.apache.ignite.internal.schema.definition.TableDefinitionImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.lang.ColumnAlreadyExistsException;
import org.apache.ignite.lang.ColumnNotFoundException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IndexAlreadyExistsException;
import org.apache.ignite.lang.IndexNotFoundException;
import org.apache.ignite.lang.LoggerMessageHelper;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnDefinition;
import org.apache.ignite.schema.definition.builder.PrimaryKeyDefinitionBuilder;
import org.apache.ignite.schema.definition.builder.SortedIndexDefinitionBuilder;
import org.apache.ignite.schema.definition.builder.SortedIndexDefinitionBuilder.SortedIndexColumnBuilder;

/** DDL commands handler. */
public class DdlCommandHandler {
    private final TableManager tableManager;

    public DdlCommandHandler(TableManager tblManager) {
        tableManager = tblManager;
    }

    /** Handles ddl commands. */
    public void handle(DdlCommand cmd, PlanningContext pctx) throws IgniteInternalCheckedException {
        validateCommand(cmd);

        if (cmd instanceof CreateTableCommand) {
            handleCreateTable((CreateTableCommand) cmd);
        } else if (cmd instanceof DropTableCommand) {
            handleDropTable((DropTableCommand) cmd);
        } else if (cmd instanceof AlterTableAddCommand) {
            handleAlterAddColumn((AlterTableAddCommand) cmd);
        } else if (cmd instanceof AlterTableDropCommand) {
            handleAlterDropColumn((AlterTableDropCommand) cmd);
        } else if (cmd instanceof CreateIndexCommand) {
            handleCreateIndex((CreateIndexCommand) cmd);
        } else if (cmd instanceof DropIndexCommand) {
            handleDropIndex((DropIndexCommand) cmd);
        } else {
            throw new IgniteInternalCheckedException("Unsupported DDL operation ["
                    + "cmdName=" + (cmd == null ? null : cmd.getClass().getSimpleName()) + "; "
                    + "querySql=\"" + pctx.query() + "\"]");
        }
    }

    /** Validate command. */
    private void validateCommand(DdlCommand cmd) {
        if (cmd instanceof AbstractDdlCommand) {
            AbstractDdlCommand cmd0 = (AbstractDdlCommand) cmd;

            if (IgniteUtils.nullOrEmpty(cmd0.tableName())) {
                throw new IllegalArgumentException("Table name is undefined.");
            }
        }
    }

    /** Handles create table command. */
    private void handleCreateTable(CreateTableCommand cmd) {
        PrimaryKeyDefinitionBuilder pkeyDef = SchemaBuilders.primaryKey();
        pkeyDef.withColumns(cmd.primaryKeyColumns());
        pkeyDef.withAffinityColumns(cmd.affColumns());

        Consumer<TableChange> tblChanger = tblCh -> {
            TableChange conv = convert(SchemaBuilders.tableBuilder(cmd.schemaName(), cmd.tableName())
                    .columns(cmd.columns())
                    .withPrimaryKey(pkeyDef.build()).build(), tblCh);

            if (cmd.partitions() != null) {
                conv.changePartitions(cmd.partitions());
            }

            if (cmd.replicas() != null) {
                conv.changeReplicas(cmd.replicas());
            }
        };

        String fullName = TableDefinitionImpl.canonicalName(cmd.schemaName(), cmd.tableName());

        if (cmd.ifTableNotExists()) {
            tableManager.createTableIfNotExists(fullName, tblChanger);
        } else {
            tableManager.createTable(fullName, tblChanger);
        }
    }

    /** Handles drop table command. */
    private void handleDropTable(DropTableCommand cmd) {
        String fullName = TableDefinitionImpl.canonicalName(cmd.schemaName(), cmd.tableName());

        // if (!cmd.ifTableExists()) todo will be implemented after IGNITE-15926

        tableManager.dropTable(fullName);
    }

    /** Handles add column command. */
    private void handleAlterAddColumn(AlterTableAddCommand cmd) {
        if (nullOrEmpty(cmd.columns())) {
            return;
        }

        // if (!cmd.ifTableExists()) todo will be implemented after IGNITE-15926

        String fullName = TableDefinitionImpl.canonicalName(cmd.schemaName(), cmd.tableName());

        addColumnInternal(fullName, cmd.columns(), cmd.ifColumnNotExists());
    }

    /** Handles drop column command. */
    private void handleAlterDropColumn(AlterTableDropCommand cmd) {
        if (nullOrEmpty(cmd.columns())) {
            return;
        }

        // if (!cmd.ifTableExists()) todo will be implemented after IGNITE-15926

        String fullName = TableDefinitionImpl.canonicalName(cmd.schemaName(), cmd.tableName());

        dropColumnInternal(fullName, cmd.columns(), cmd.ifColumnExists());
    }

    /** Handles create index command. */
    private void handleCreateIndex(CreateIndexCommand cmd) {
        // Only sorted idx for now.
        SortedIndexDefinitionBuilder idx = SchemaBuilders.sortedIndex(cmd.indexName());

        for (Pair<String, Boolean> idxInfo : cmd.columns()) {
            SortedIndexColumnBuilder idx0 = idx.addIndexColumn(idxInfo.getFirst());

            if (idxInfo.getSecond()) {
                idx0.desc();
            }

            idx0.done();
        }

        String fullName = TableDefinitionImpl.canonicalName(cmd.schemaName(), cmd.tableName());

        tableManager.alterTable(fullName, chng -> chng.changeIndices(idxes -> {
            if (idxes.get(cmd.indexName()) != null) {
                if (!cmd.ifIndexNotExists()) {
                    throw new IndexAlreadyExistsException(cmd.indexName());
                } else {
                    return;
                }
            }

            idxes.create(cmd.indexName(), tableIndexChange -> convert(idx.build(), tableIndexChange));
        }));
    }

    /** Handles drop index command. */
    private void handleDropIndex(DropIndexCommand cmd) {
        String fullName = TableDefinitionImpl.canonicalName(cmd.schemaName(), cmd.tableName());

        tableManager.alterTable(fullName, chng -> chng.changeIndices(idxes -> {
            if (idxes.get(cmd.indexName()) == null) {
                if (!cmd.ifExist()) {
                    throw new IndexNotFoundException(cmd.indexName(), fullName);
                } else {
                    return;
                }
            }

            idxes.delete(cmd.indexName());
        }));
    }

    /**
     * Adds a column according to the column definition.
     *
     * @param fullName Table with schema name.
     * @param colsDef  Columns defenitions.
     * @param colNotExist Flag indicates exceptionally behavior in case of already existing column.
     */
    private void addColumnInternal(String fullName, List<ColumnDefinition> colsDef, boolean colNotExist) {
        tableManager.alterTable(
                fullName,
                chng -> chng.changeColumns(cols -> {
                    Map<String, String> colNamesToOrders = columnOrdersToNames(chng.columns());

                    List<ColumnDefinition> colsDef0;

                    if (!colNotExist) {
                        colsDef.stream().filter(k -> colNamesToOrders.containsKey(k.name())).findAny()
                                .ifPresent(c -> {
                                    throw new ColumnAlreadyExistsException(c.name());
                                });

                        colsDef0 = colsDef;
                    } else {
                        colsDef0 = colsDef.stream().filter(k -> !colNamesToOrders.containsKey(k.name())).collect(Collectors.toList());
                    }

                    for (ColumnDefinition colDef : colsDef0) {
                        cols.create(colDef.name(), colChg -> convert(colDef, colChg));
                    }
                }));
    }

    /**
     * Drops a column(s) exceptional behavior depends on {@code colExist} flag.
     *
     * @param fullName Table with schema name.
     * @param colNames Columns defenitions.
     * @param colExist Flag indicates exceptionally behavior in case of already existing column.
     */
    private void dropColumnInternal(String fullName, Set<String> colNames, boolean colExist) {
        tableManager.alterTable(
                fullName,
                chng -> chng.changeColumns(cols -> {
                    PrimaryKeyView priKey = chng.primaryKey();

                    Map<String, String> colNamesToOrders = columnOrdersToNames(chng.columns());

                    Set<String> colNames0 = new HashSet<>();

                    for (String colName : colNames) {
                        if (!colNamesToOrders.containsKey(colName)) {
                            if (!colExist) {
                                throw new ColumnNotFoundException(colName, fullName);
                            }
                        } else {
                            colNames0.add(colName);
                        }

                        for (String priColName : priKey.columns()) {
                            if (priColName.equals(colName)) {
                                throw new IgniteException(LoggerMessageHelper
                                        .format("Can`t delete column, belongs to primary key: [name={}]", colName));
                            }
                        }
                    }

                    colNames0.forEach(k -> cols.delete(colNamesToOrders.get(k)));
                }));
    }

    /** Map column names to orders. */
    private static Map<String, String> columnOrdersToNames(NamedListView<? extends ColumnView> cols) {
        Map<String, String> colNames = new HashMap<>(cols.size());

        for (String colOrder : cols.namedListKeys()) {
            colNames.put(cols.get(colOrder).name(), colOrder);
        }

        return colNames;
    }
}
