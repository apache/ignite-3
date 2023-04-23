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

package org.apache.ignite.internal.schema.configuration.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesView;

/**
 * Index configuration validator implementation.
 */
public class IndexValidatorImpl implements Validator<IndexValidator, NamedListView<TableIndexView>> {
    /** Static instance. */
    public static final IndexValidatorImpl INSTANCE = new IndexValidatorImpl();

    @Override
    public void validate(IndexValidator annotation, ValidationContext<NamedListView<TableIndexView>> ctx) {
        TablesView tablesConfig = ctx.getNewRoot(TablesConfiguration.KEY);

        assert tablesConfig != null;

        NamedListView<? extends TableIndexView> indexView = tablesConfig.indexes();

        NamedListView<? extends TableView> tablesView = tablesConfig.tables();

        Set<String> tblNames = new HashSet<>(tablesView.namedListKeys());

        for (String key : newKeys(ctx.getOldValue(), ctx.getNewValue())) {
            if (tblNames.contains(key)) {
                ctx.addIssue(new ValidationIssue(key, "Unable to create index. Table with the same name already exists."));
            }

            TableIndexView idxView = indexView.get(key);

            UUID tableId = idxView.tableId();

            TableView tableView = tablesView.get(tableId);

            if (tableView == null) {
                ctx.addIssue(new ValidationIssue(key, "Unable to create index [name=" + key + "]. Table not found."));

                // no further validation required for current index
                continue;
            }

            List<String> colocationColumns;
            if (tableView.primaryKey() != null) {
                colocationColumns = List.of(tableView.primaryKey().colocationColumns());
            } else {
                colocationColumns = List.of();
            }

            validate(ctx, idxView, tableView.columns().namedListKeys(), colocationColumns);
        }
    }

    private void validate(
            ValidationContext<?> ctx,
            TableIndexView indexView,
            Collection<String> tableColumns,
            Collection<String> collocationColumns
    ) {
        List<String> indexedColumns;
        if (indexView instanceof HashIndexView) {
            var index0 = (HashIndexView) indexView;

            // we need modifiable list
            indexedColumns = new ArrayList<>(Arrays.asList(index0.columnNames()));
        } else if (indexView instanceof SortedIndexView) {
            var index0 = (SortedIndexView) indexView;

            // we need modifiable list
            indexedColumns = new ArrayList<>(index0.columns().namedListKeys());
        } else {
            ctx.addIssue(new ValidationIssue(indexView.name(), "Index type is not supported [type=" + indexView.type() + "]"));

            // no further validation required
            return;
        }

        if (indexedColumns.isEmpty()) {
            ctx.addIssue(new ValidationIssue(indexView.name(), "Index must include at least one column"));

            // no further validation required
            return;
        }

        // need check this first because later indexedColumns will be truncated
        if (indexView.uniq()) {
            if (collocationColumns.isEmpty()) {
                ctx.addIssue(new ValidationIssue(indexView.name(), "Unique index is not supported for tables without primary key"));
            } else if (!indexedColumns.containsAll(collocationColumns)) {
                ctx.addIssue(new ValidationIssue(indexView.name(), "Unique index must include all colocation columns"));
            }
        }

        indexedColumns.removeAll(tableColumns);

        if (!indexedColumns.isEmpty()) {
            ctx.addIssue(new ValidationIssue(indexView.name(), "Columns don't exist [columns=" + indexedColumns + "]"));
        }
    }

    private List<String> newKeys(NamedListView<?> before, NamedListView<?> after) {
        List<String> result = new ArrayList<>(after.namedListKeys());

        if (before != null) {
            result.removeAll(before.namedListKeys());
        }

        return result;
    }

    /** Private constructor. */
    private IndexValidatorImpl() {
    }
}
