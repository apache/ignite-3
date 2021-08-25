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
import static org.apache.ignite.internal.util.IgniteUtils.isNullOrEmpty;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.schemas.table.ColumnView;
import org.apache.ignite.configuration.schemas.table.PrimaryKeyView;
import org.apache.ignite.configuration.schemas.table.TableChange;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.AbstractAlterTableCommand;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.AlterTableAddCommand;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.AlterTableDropCommand;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.CreateIndexCommand;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.CreateTableCommand;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.DdlCommand;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.DropIndexCommand;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.DropTableCommand;
import org.apache.ignite.internal.schema.definition.TableDefinitionImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.lang.ColumnAlreadyExistsException;
import org.apache.ignite.lang.ColumnNotFoundException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IndexAlreadyExistsException;
import org.apache.ignite.lang.LoggerMessageHelper;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.definition.ColumnDefinition;
import org.apache.ignite.schema.definition.builder.PrimaryKeyDefinitionBuilder;
import org.apache.ignite.schema.definition.index.IndexDefinition;

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
        if (cmd instanceof AbstractAlterTableCommand) {
            AbstractAlterTableCommand cmd0 = (AbstractAlterTableCommand) cmd;
    
            if (isNullOrEmpty(cmd0.tableName())) {
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
        
        if (cmd.ifNotExists()) {
            tableManager.createTableIfNotExists(fullName, tblChanger);
        } else {
            tableManager.createTable(fullName, tblChanger);
        }
    }
    
    /** Handles drop table command. */
    private void handleDropTable(DropTableCommand cmd) {
        String fullName = TableDefinitionImpl.canonicalName(cmd.schemaName(), cmd.tableName());
        
        // if (!cmd.ifExists()) todo will be implemented after IGNITE-15926
        
        tableManager.dropTable(fullName);
    }
    
    /** Handles add column command. */
    private void handleAlterAddColumn(AlterTableAddCommand cmd) {
        if (nullOrEmpty(cmd.columns())) {
            return;
        }
        
        String fullName = TableDefinitionImpl.canonicalName(cmd.schemaName(), cmd.tableName());
        
        try {
            addColumnInternal(fullName, cmd.columns());
        } catch (ColumnAlreadyExistsException ex) {
            if (!cmd.ifColumnNotExists()) {
                throw ex;
            }
        }
    }
    
    /** Handles drop column command. */
    private void handleAlterDropColumn(AlterTableDropCommand cmd) {
        if (nullOrEmpty(cmd.columns())) {
            return;
        }
        
        String fullName = TableDefinitionImpl.canonicalName(cmd.schemaName(), cmd.tableName());
        
        try {
            dropColumnInternal(fullName, cmd.columns());
        } catch (ColumnNotFoundException ex) {
            if (!cmd.ifColumnExists()) {
                throw ex;
            }
        }
    }
    
    /** Handles create index command. */
    private void handleCreateIndex(CreateIndexCommand cmd) {
        IndexDefinition idx = SchemaBuilders.hashIndex(cmd.indexName())
                .withColumns(cmd.columns().stream().map(Pair::getFirst).collect(Collectors.toSet()))
                .build();
        
        String fullName = TableDefinitionImpl.canonicalName(cmd.schemaName(), cmd.tableName());
        
        tableManager.alterTable(fullName, chng -> chng.changeIndices(idxes -> {
            if (idxes.get(idx.name()) != null) {
                throw new IndexAlreadyExistsException(idx.name());
            }
            
            idxes.create(idx.name(), tableIndexChange -> convert(idx, tableIndexChange));
        }));
    }
    
    /** Handles create index command. */
    private void handleDropIndex(DropIndexCommand cmd) {
        String fullName = TableDefinitionImpl.canonicalName(cmd.schemaName(), cmd.tableName());
        
        tableManager.alterTable(fullName, chng -> chng.changeIndices(idxes -> {
            if (idxes.get(cmd.indexName()) == null) {
                if (!cmd.ifExist()) {
                    throw new IndexAlreadyExistsException(cmd.indexName());
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
     */
    private void addColumnInternal(String fullName, Set<ColumnDefinition> colsDef) {
        tableManager.alterTable(
                fullName,
                chng -> chng.changeColumns(cols -> {
                    Map<String, String> colNamesToOrders = columnOrdersToNames(chng.columns());
                    
                    colsDef.stream().filter(k -> colNamesToOrders.containsKey(k.name())).findAny()
                            .ifPresent(c -> {
                                throw new ColumnAlreadyExistsException(c.name());
                            });
                    
                    int colIdx = chng.columns().namedListKeys().stream().mapToInt(Integer::parseInt).max().getAsInt();
                    
                    for (ColumnDefinition colDef : colsDef) {
                        colIdx += 1;
                        cols.create(String.valueOf(colIdx), colChg -> convert(colDef, colChg));
                    }
                }));
    }
    
    /**
     * Adds a column according to the column definition.
     *
     * @param fullName Table with schema name.
     * @param colsDef  Columns defenitions.
     */
    private void dropColumnInternal(String fullName, Set<String> colsDef) {
        tableManager.alterTable(
                fullName,
                chng -> chng.changeColumns(cols -> {
                    PrimaryKeyView priKey = chng.primaryKey();
                    
                    Set<String> pkey0 = new HashSet<>(Arrays.asList(priKey.columns()));
                    
                    Map<String, String> colNamesToOrders = columnOrdersToNames(chng.columns());
                    
                    for (String colName : colsDef) {
                        if (!colNamesToOrders.keySet().contains(colName)) {
                            throw new ColumnNotFoundException(colName);
                        }
                        
                        if (pkey0.contains(colName)) {
                            throw new IgniteException(LoggerMessageHelper
                                    .format("Can`t delete column, belongs to primary key: [name={}]", colName));
                        }
                    }
                    
                    colsDef.forEach(k -> cols.delete(colNamesToOrders.get(k)));
                }));
    }
    
    /** Form column names from orders. */
    private static Map<String, String> columnOrdersToNames(NamedListView<? extends ColumnView> cols) {
        Map<String, String> colNames = new HashMap<>(cols.size());
        
        for (String colOrder : cols.namedListKeys()) {
            colNames.put(cols.get(colOrder).name(), colOrder);
        }
        
        return colNames;
    }
}
