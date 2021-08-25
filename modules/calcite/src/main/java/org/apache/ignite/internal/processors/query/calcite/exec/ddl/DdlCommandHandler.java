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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.schemas.table.IndexColumnView;
import org.apache.ignite.configuration.schemas.table.TableIndexView;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.AbstractAlterTableCommand;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.AlterTableAddCommand;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.AlterTableDropCommand;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.CreateTableCommand;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.DdlCommand;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.DropTableCommand;
import org.apache.ignite.internal.schema.SchemaTableImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.LoggerMessageHelper;
import org.apache.ignite.schema.Column;
import org.apache.ignite.schema.SchemaBuilders;
import org.apache.ignite.schema.SchemaTable;
import org.apache.ignite.schema.builder.PrimaryIndexBuilder;
import org.apache.ignite.table.Table;

import static org.apache.ignite.internal.schema.configuration.SchemaConfigurationConverter.convert;
import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;
import static org.apache.ignite.internal.util.IgniteUtils.isNullOrEmpty;
import static org.apache.ignite.schema.PrimaryIndex.PRIMARY_KEY_INDEX_NAME;

/** DDL commands handler. */
public class DdlCommandHandler {
    /** */
    private final TableManager tableManager;

    /** */
    public DdlCommandHandler(TableManager tblManager) {
        tableManager = tblManager;
    }

    /** */
    public void handle(DdlCommand cmd, PlanningContext pctx) throws IgniteInternalCheckedException {
        validateCommand(cmd);

        if (cmd instanceof CreateTableCommand)
            handleCreateTable((CreateTableCommand)cmd);
        else if (cmd instanceof DropTableCommand)
            handleDropTable((DropTableCommand)cmd);
        else if (cmd instanceof AlterTableAddCommand)
            handleAlterAddColumn((AlterTableAddCommand)cmd);
        else if (cmd instanceof AlterTableDropCommand)
            handleAlterDropColumn((AlterTableDropCommand)cmd);
        else {
            throw new IgniteInternalCheckedException("Unsupported DDL operation [" +
                "cmdName=" + (cmd == null ? null : cmd.getClass().getSimpleName()) + "; " +
                "querySql=\"" + pctx.query() + "\"]");
        }
    }

    /** Validate command. */
    private void validateCommand(DdlCommand cmd) {
        if (cmd instanceof AbstractAlterTableCommand) {
            AbstractAlterTableCommand cmd0 = (AbstractAlterTableCommand)cmd;

            if (isNullOrEmpty(cmd0.tableName()))
                throw new IllegalArgumentException("Table name is undefined.");
        }
    }

    /** */
    private void handleCreateTable(CreateTableCommand cmd) throws IgniteInternalCheckedException {
        if (tableManager.table(SchemaTableImpl.canonicalName(cmd.schemaName(), cmd.tableName())) != null) {
            if (!cmd.ifNotExists())
                throw new IgniteInternalCheckedException(LoggerMessageHelper.format(
                    "Table already exists [name={}]", cmd.tableName()));
        }
        else {
            PrimaryIndexBuilder pkIdx = SchemaBuilders.pkIndex();

            cmd.primaryKeyColumns().forEach(k -> pkIdx.addIndexColumn(k).done());

            SchemaTable tableSchm = SchemaBuilders.tableBuilder(cmd.schemaName(), cmd.tableName())
                .columns(cmd.columns()).withIndex(pkIdx.build()).build();

            tableManager.createTable(
                tableSchm.canonicalName(),
                tbl -> convert(tableSchm, tbl)
            );
        }
    }

    /** */
    private void handleDropTable(DropTableCommand cmd) throws IgniteInternalCheckedException {
        String canonicalName = SchemaTableImpl.canonicalName(cmd.schemaName(), cmd.tableName());

        if (tableManager.table(canonicalName) == null) {
            if (!cmd.ifExists())
                throw new IgniteInternalCheckedException(LoggerMessageHelper.format(
                    "Table not exists [name={}]", cmd.tableName()));
        }

        tableManager.dropTable(canonicalName);
    }

    /** */
    private void handleAlterAddColumn(AlterTableAddCommand cmd) throws IgniteInternalCheckedException {
        if (nullOrEmpty(cmd.columns()))
            return;

        String canonicalName = SchemaTableImpl.canonicalName(cmd.schemaName(), cmd.tableName());

        Table tbl = tableManager.table(canonicalName);

        if (tbl == null) {
            throw new IgniteInternalCheckedException(LoggerMessageHelper.format(
                "Table not exists, name={}", cmd.tableName()));
        }

        try {
            tableManager.alterTable(canonicalName, tblCh -> tblCh.changeColumns(columns -> {
                for (Column col0 : cmd.columns())
                    columns.create(col0.name(), colChg -> convert(col0, colChg));
            }));
        }
        catch (Throwable e) {
            if (!cmd.ifColumnNotExists())
                throw e;
        }
    }

    /** */
    private void handleAlterDropColumn(AlterTableDropCommand cmd) throws IgniteInternalCheckedException {
        if (nullOrEmpty(cmd.columns()))
            return;

        String canonicalName = SchemaTableImpl.canonicalName(cmd.schemaName(), cmd.tableName());

        if (tableManager.table(canonicalName) == null) {
            throw new IgniteInternalCheckedException(LoggerMessageHelper.format(
                "Table not exists [name={}]", cmd.tableName()));
        }

        Set<String> dropCols = new HashSet<>(cmd.columns().size());

        dropCols.addAll(cmd.columns());

        try {
            tableManager.alterTable(canonicalName, chng -> chng.changeColumns(cols -> {
                TableIndexView pkIdx = chng.indices().namedListKeys().stream()
                    .filter(k -> chng.indices().get(k).name().equals(PRIMARY_KEY_INDEX_NAME))
                    .map(k -> chng.indices().get(k))
                    .findAny()
                    .orElseThrow(() -> {
                        throw new IllegalStateException("Primary key index not found.");
                    });

                Set<String> tblCols = chng.columns().namedListKeys().stream().map(k -> chng.columns().get(k).name())
                    .collect(Collectors.toSet());

                Collection<String> issues = null;

                for (String colName : dropCols) {
                    if (!tblCols.contains(colName)) {
                        issues = Objects.requireNonNullElseGet(issues, ArrayList::new);

                        issues.add(colName);
                    }
                }

                if (issues != null)
                    throw new IllegalStateException(
                        LoggerMessageHelper.format("Columns not found, columns=[{}]",
                            String.join(", ", issues)));

                List<String> dropColsIntersect = pkIdx.columns().namedListKeys().stream()
                    .map(k -> pkIdx.columns().get(k))
                    .filter(k -> dropCols.contains(k.name()))
                    .map(IndexColumnView::name)
                    .collect(Collectors.toList());

                if (!nullOrEmpty(dropColsIntersect))
                    throw new IllegalStateException(
                        LoggerMessageHelper.format("Can`t drop columns from pk index, columns=[{}]",
                            String.join(", ", dropColsIntersect)));
                else
                    dropCols.forEach(cols::delete);
            }));
        }
        catch (Exception e) {
            if (!cmd.ifColumnExists())
                throw e;
        }
    }
}
