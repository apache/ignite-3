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

package org.apache.ignite.internal.schema.configuration;

import static org.apache.ignite.internal.util.ArrayUtils.nullOrEmpty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;

/**
 * Table schema configuration validator implementation.
 */
public class TableValidatorImpl implements Validator<TableValidator, NamedListView<TableView>> {
    /** Static instance. */
    public static final TableValidatorImpl INSTANCE = new TableValidatorImpl();

    /** {@inheritDoc} */
    @Override
    public void validate(TableValidator annotation, ValidationContext<NamedListView<TableView>> ctx) {
        TablesView tablesConfig = ctx.getNewRoot(TablesConfiguration.KEY);

        Set<String> idxNames = tablesConfig == null ? Collections.emptySet() : new HashSet<>(tablesConfig.indexes().namedListKeys());

        NamedListView<TableView> newTables = ctx.getNewValue();

        for (String tableName : newKeys(ctx.getOldValue(), ctx.getNewValue())) {
            if (idxNames.contains(tableName)) {
                ctx.addIssue(new ValidationIssue(tableName, "Unable to create table. Index with the same name already exists."));
            }

            TableView newTable = newTables.get(tableName);

            if (newTable.columns() == null || newTable.columns().size() == 0) {
                ctx.addIssue(new ValidationIssue(newTable.name(), "Table should include at least one column"));

                // no further validation required
                return;
            }

            Set<String> columnsDups = findDuplicates(newTable.columns().namedListKeys());

            if (!columnsDups.isEmpty()) {
                ctx.addIssue(new ValidationIssue(newTable.name(),
                        "Some columns are specified more than once [duplicates=" + new HashSet<>(columnsDups) + "]"));
            }

            Set<String> columns = new HashSet<>(newTable.columns().namedListKeys());

            if (newTable.primaryKey() == null || nullOrEmpty(newTable.primaryKey().columns())) {
                ctx.addIssue(new ValidationIssue(newTable.name(), "Table without primary key is not supported"));

                // no further validation required
                return;
            }

            List<String> pkColumns = Arrays.asList(newTable.primaryKey().columns());

            if (!columns.containsAll(pkColumns)) {
                Set<String> missingColumns = new HashSet<>(pkColumns);

                missingColumns.removeAll(columns);

                ctx.addIssue(new ValidationIssue(newTable.name(), "Columns don't exists [columns=" + missingColumns + "]"));
            }

            Set<String> pkDups = findDuplicates(pkColumns);
            if (!pkDups.isEmpty()) {
                ctx.addIssue(new ValidationIssue(newTable.name(),
                        "Primary key contains duplicated columns [duplicates=" + pkDups + "]"));
            }

            if (nullOrEmpty(newTable.primaryKey().colocationColumns())) {
                ctx.addIssue(new ValidationIssue(newTable.name(), "Colocation columns must be specified"));

                // no further validation required
                return;
            }

            Set<String> outstandingColumns = new HashSet<>(Arrays.asList(newTable.primaryKey().colocationColumns()));

            outstandingColumns.removeAll(Arrays.asList(newTable.primaryKey().columns()));

            if (!outstandingColumns.isEmpty()) {
                ctx.addIssue(new ValidationIssue(newTable.name(),
                        "Colocation columns must be subset of primary key [outstandingColumns=" + outstandingColumns + "]"));
            }

            Set<String> colocationDups = findDuplicates(Arrays.asList(newTable.primaryKey().colocationColumns()));

            if (!colocationDups.isEmpty()) {
                ctx.addIssue(new ValidationIssue(newTable.name(),
                        "Colocation columns contains duplicates [duplicates=" + colocationDups + "]"));
            }
        }
    }

    /** Private constructor. */
    private TableValidatorImpl() {
    }

    private List<String> newKeys(NamedListView<?> before, NamedListView<?> after) {
        List<String> result = new ArrayList<>(after.namedListKeys());

        if (before != null) {
            result.removeAll(before.namedListKeys());
        }

        return result;
    }

    private Set<String> findDuplicates(Collection<String> collection) {
        Set<String> used = new HashSet<>();
        Set<String> result = new HashSet<>();

        for (String element : collection) {
            if (!used.add(element)) {
                result.add(element);
            }
        }

        return result;
    }
}
