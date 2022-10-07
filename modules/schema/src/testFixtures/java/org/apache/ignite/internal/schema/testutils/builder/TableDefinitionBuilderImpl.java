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

package org.apache.ignite.internal.schema.testutils.builder;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.schema.testutils.definition.ColumnDefinition;
import org.apache.ignite.internal.schema.testutils.definition.PrimaryKeyDefinition;
import org.apache.ignite.internal.schema.testutils.definition.TableDefinition;
import org.apache.ignite.internal.schema.testutils.definition.TableDefinitionImpl;
import org.apache.ignite.internal.util.IgniteNameUtils;

/**
 * Table builder.
 */
class TableDefinitionBuilderImpl implements TableDefinitionBuilder {
    /** Schema name. */
    private final String schemaName;

    /** Table name. */
    private final String tableName;

    /** Columns definitions. */
    private final LinkedHashMap<String, ColumnDefinition> columns = new LinkedHashMap<>();

    /** Table primary key. */
    private PrimaryKeyDefinition primaryKeyDefinition;

    /**
     * Constructor.
     *
     * @param schemaName Schema name.
     * @param tableName Table name.
     */
    public TableDefinitionBuilderImpl(String schemaName, String tableName) {
        this.schemaName = IgniteNameUtils.parseSimpleName(schemaName);
        this.tableName = IgniteNameUtils.parseSimpleName(tableName);
    }

    /** {@inheritDoc} */
    @Override
    public TableDefinitionBuilderImpl columns(List<ColumnDefinition> columns) {
        for (ColumnDefinition column : columns) {
            if (this.columns.put(column.name(), column) != null) {
                throw new IllegalArgumentException("Column with same name already exists: columnName=" + column.name());
            }
        }

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public TableDefinitionBuilderImpl columns(ColumnDefinition... columns) {
        return columns(Arrays.asList(columns));
    }

    /** {@inheritDoc} */
    @Override
    public TableDefinitionBuilder withPrimaryKey(String colName) {
        primaryKeyDefinition = SchemaBuilders.primaryKey().withColumns(IgniteNameUtils.parseSimpleName(colName)).build();

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public TableDefinitionBuilder withPrimaryKey(PrimaryKeyDefinition primaryKeyDefinition) {
        this.primaryKeyDefinition = primaryKeyDefinition;

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public TableDefinitionBuilder withHints(Map<String, String> hints) {
        // No op.
        return this;
    }

    /** {@inheritDoc} */
    @Override
    public TableDefinition build() {
        assert schemaName != null : "Database schema name must be specified.";

        assert primaryKeyDefinition != null : "Primary key index must be configured.";
        assert columns.size() > primaryKeyDefinition.columns().size() : "Key or/and value columns must be defined.";

        return new TableDefinitionImpl(
                schemaName,
                tableName,
                columns,
                primaryKeyDefinition
        );
    }
}
