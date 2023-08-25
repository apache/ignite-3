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

package org.apache.ignite.internal.catalog.commands.builders;

import static java.util.Objects.requireNonNullElse;

import java.util.List;
import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.commands.ColumnParams;
import org.apache.ignite.internal.catalog.commands.CreateTableCommand;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of {@link CreateTableCommandBuilder}.
 */
public class CreateTableCommandBuilderImpl implements CreateTableCommandBuilder {
    private @Nullable List<ColumnParams> columns;

    private @Nullable String schemaName;

    private @Nullable String tableName;

    private @Nullable List<String> primaryKeyColumns;

    private @Nullable List<String> colocationColumns;

    private @Nullable String zoneName;

    @Override
    public CreateTableCommandBuilder schemaName(String schemaName) {
        this.schemaName = schemaName;

        return this;
    }

    @Override
    public CreateTableCommandBuilder tableName(String tableName) {
        this.tableName = tableName;

        return this;
    }

    @Override
    public CreateTableCommandBuilder columns(List<ColumnParams> columns) {
        this.columns = columns;

        return this;
    }

    @Override
    public CreateTableCommandBuilder primaryKeyColumns(List<String> primaryKeyColumns) {
        this.primaryKeyColumns = primaryKeyColumns;

        return this;
    }

    @Override
    public CreateTableCommandBuilder colocationColumns(List<String> colocationColumns) {
        this.colocationColumns = colocationColumns;

        return this;
    }

    @Override
    public CreateTableCommandBuilder zone(String zoneName) {
        this.zoneName = zoneName;

        return this;
    }

    @SuppressWarnings("DataFlowIssue") // params are validated in constructor
    @Override
    public CatalogCommand build() {
        String zoneName = requireNonNullElse(this.zoneName, CatalogService.DEFAULT_ZONE_NAME);

        if (colocationColumns == null) {
            colocationColumns = primaryKeyColumns;
        }

        return new CreateTableCommand(
                tableName,
                schemaName,
                primaryKeyColumns,
                colocationColumns,
                columns,
                zoneName
        );
    }
}
