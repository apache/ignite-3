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

import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.getByInternalId;

import java.util.List;
import java.util.UUID;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.schemas.table.IndexValidator;
import org.apache.ignite.configuration.schemas.table.TableIndexView;
import org.apache.ignite.configuration.schemas.table.TableView;
import org.apache.ignite.configuration.schemas.table.TablesConfiguration;
import org.apache.ignite.configuration.schemas.table.TablesView;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.schema.definition.SchemaValidationUtils;
import org.apache.ignite.internal.schema.definition.TableDefinitionImpl;
import org.apache.ignite.schema.definition.ColumnDefinition;
import org.apache.ignite.schema.definition.index.IndexDefinition;

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

        for (String key : indexView.namedListKeys()) {
            TableIndexView idxView = indexView.get(key);

            UUID tableId = idxView.tableId();

            TableView tableView = getByInternalId(tablesView, tableId);

            assert tableView != null;

            IndexDefinition index = SchemaConfigurationConverter.convert(idxView);

            TableDefinitionImpl tbl = SchemaConfigurationConverter.convert(tableView);

            List<ColumnDefinition> tableColumns = tbl.columns();

            List<String> tableColocationColumns = tbl.colocationColumns();

            try {
                SchemaValidationUtils.validateIndices(index, tableColumns, tableColocationColumns);
            } catch (IllegalStateException e) {
                ctx.addIssue(new ValidationIssue(key, e.getMessage()));
            }
        }
    }

    /** Private constructor. */
    private IndexValidatorImpl() {
    }
}
