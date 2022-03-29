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

package org.apache.ignite.internal.schema.configuration;

import java.util.Objects;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.schemas.table.ColumnView;
import org.apache.ignite.configuration.schemas.table.TableIndexView;
import org.apache.ignite.configuration.schemas.table.TableValidator;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.schema.definition.TableDefinitionImpl;
import org.apache.ignite.internal.schema.definition.builder.TableDefinitionBuilderImpl;
import org.jetbrains.annotations.Nullable;

/**
 * Table schema configuration validator implementation.
 */
public class TableValidatorImpl implements Validator<TableValidator, NamedListView<TableView>> {
    /** Static instance. */
    public static final TableValidatorImpl INSTANCE = new TableValidatorImpl();

    /** {@inheritDoc} */
    @Override
    public void validate(TableValidator annotation, ValidationContext<NamedListView<TableView>> ctx) {
        NamedListView<TableView> oldTables = ctx.getOldValue();
        NamedListView<TableView> newTables = ctx.getNewValue();

        for (String tableName : newTables.namedListKeys()) {
            TableView newTable = newTables.get(tableName);

            try {
                TableDefinitionImpl tbl = SchemaConfigurationConverter.convert(newTable);

                assert !tbl.keyColumns().isEmpty();
                assert !tbl.colocationColumns().isEmpty();

                TableDefinitionBuilderImpl.validateIndices(tbl.indices(), tbl.columns(), tbl.colocationColumns());
            } catch (IllegalArgumentException e) {
                ctx.addIssue(new ValidationIssue("Validator works success by key " + ctx.currentKey() + ". Found "
                        + newTable.columns().size() + " columns"));
            }

            validateDataStorage(oldTables == null ? null : oldTables.get(tableName), newTable, ctx);

            validateNamedListKeys(newTable, ctx);
        }
    }

    private static void validateDataStorage(@Nullable TableView oldTable, TableView newTable, ValidationContext<?> ctx) {
        if (oldTable != null) {
            String oldDataStorage = oldTable.dataStorage().name();
            String newDataStorage = newTable.dataStorage().name();

            if (!Objects.equals(oldDataStorage, newDataStorage)) {
                ctx.addIssue(new ValidationIssue(String.format(
                        "Unable to change data storage from '%s' to '%s' for table '%s'",
                        oldDataStorage,
                        newDataStorage,
                        newTable.name()
                )));
            }
        }
    }

    /**
     * Checks that columns and indices are stored under their names in the corresponding Named Lists, i.e. their Named List keys and names
     * are the same.
     *
     * @param table Table configuration view.
     * @param ctx Validation context.
     */
    private static void validateNamedListKeys(TableView table, ValidationContext<NamedListView<TableView>> ctx) {
        NamedListView<? extends ColumnView> columns = table.columns();

        for (String key : columns.namedListKeys()) {
            ColumnView column = columns.get(key);

            if (!column.name().equals(key)) {
                var issue = new ValidationIssue(String.format(
                        "Column name \"%s\" does not match its Named List key: \"%s\"", column.name(), key
                ));

                ctx.addIssue(issue);
            }
        }

        NamedListView<? extends TableIndexView> indices = table.indices();

        for (String key : indices.namedListKeys()) {
            TableIndexView index = indices.get(key);

            if (!index.name().equals(key)) {
                var issue = new ValidationIssue(String.format(
                        "Index name \"%s\" does not match its Named List key: \"%s\"", index.name(), key
                ));

                ctx.addIssue(issue);
            }
        }
    }

    /** Private constructor. */
    private TableValidatorImpl() {
    }
}
