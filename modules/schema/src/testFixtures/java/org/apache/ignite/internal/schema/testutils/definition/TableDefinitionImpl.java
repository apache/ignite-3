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

package org.apache.ignite.internal.schema.testutils.definition;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.schema.SchemaUtils;
import org.apache.ignite.internal.tostring.S;

/**
 * Table.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class TableDefinitionImpl extends AbstractSchemaObject implements TableDefinition {
    /** Schema name. */
    private final String schemaName;

    /** Key columns. */
    private final LinkedHashMap<String, ColumnDefinition> colMap;

    /** Cached key columns. */
    private final Set<String> keyCols;

    /** Colocation columns. */
    private final List<String> colocationCols;

    /**
     * Constructor.
     *
     * @param schemaName           Schema name.
     * @param tableName            Table name.
     * @param colMap               Columns.
     * @param primaryKeyDefinition Primary key.
     */
    public TableDefinitionImpl(
            String schemaName,
            String tableName,
            LinkedHashMap<String, ColumnDefinition> colMap,
            PrimaryKeyDefinition primaryKeyDefinition
    ) {
        super(tableName);

        this.schemaName = schemaName;
        this.colMap = colMap;

        keyCols = primaryKeyDefinition.columns();
        colocationCols = primaryKeyDefinition.colocationColumns();
    }

    /** {@inheritDoc} */
    @Override
    public String schemaName() {
        return schemaName;
    }

    /** {@inheritDoc} */
    @Override
    public Set<String> keyColumns() {
        return keyCols;
    }

    /** {@inheritDoc} */
    @Override
    public List<String> colocationColumns() {
        return colocationCols;
    }

    /** {@inheritDoc} */
    @Override
    public List<ColumnDefinition> columns() {
        return new ArrayList<>(colMap.values());
    }

    /** {@inheritDoc} */
    @Override
    public String canonicalName() {
        return SchemaUtils.canonicalName(schemaName, name());
    }

    /**
     * Check if specified column already exists.
     *
     * @param name Column name.
     * @return {@code True} if column with given name already exists, {@code false} otherwise.
     */
    public boolean hasColumn(String name) {
        return colMap.containsKey(name);
    }

    /**
     * Check if specified key column already exists.
     *
     * @param name Column name.
     * @return {@code True} if key column with given name already exists, {@code false} otherwise.
     */
    public boolean hasKeyColumn(String name) {
        return keyCols.contains(name);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(TableDefinitionImpl.class, this);
    }
}
