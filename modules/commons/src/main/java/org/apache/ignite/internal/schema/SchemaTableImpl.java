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

package org.apache.ignite.internal.schema;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.modification.TableModificationBuilderImpl;
import org.apache.ignite.schema.Column;
import org.apache.ignite.schema.SchemaTable;
import org.apache.ignite.schema.TableIndex;
import org.apache.ignite.schema.modification.TableModificationBuilder;

/**
 * Table.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class SchemaTableImpl implements SchemaTable {
    /** Schema name. */
    private final String schemaName;

    /** Table name. */
    private final String tableName;

    /** Key columns. */
    private final Map<String, Column> cols;

    /** Key affinity columns. */
    final Set<String> keyColNames;

    /** Value columns. */
    final Set<String> affColNames;

    /** Indices. */
    private final Map<String, TableIndex> indices;

    /**
     * Constructor.
     *
     * @param schemaName Schema name.
     * @param tableName Table name.
     * @param columns Columns.
     * @param keyColNames Key columns names.
     * @param affColNames Affinity key columns names.
     * @param indices Indices.
     */
    public SchemaTableImpl(
        String schemaName,
        String tableName,
        final LinkedHashMap<String, Column> columns,
        final Set<String> keyColNames,
        final Set<String> affColNames,
        final Map<String, TableIndex> indices
    ) {
        this.schemaName = schemaName;
        this.tableName = tableName;

        this.cols = Collections.unmodifiableMap(
            columns.values().stream().filter(c -> keyColNames.contains(c.name())).collect(Collectors.toMap(
                Column::name,
                Function.identity(),
                (c1, c2) -> {
                    throw new IllegalStateException("Duplicate key column.");
                },
                LinkedHashMap::new
            )));

        this.keyColNames = keyColNames;
        this.affColNames = affColNames;

        this.indices = indices;
    }

    /** {@inheritDoc} */
    @Override public Collection<Column> keyColumns() {
        return cols.values().stream().filter(c -> keyColNames.contains(c.name())).collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override public Collection<Column> affinityColumns() {
        return cols.values().stream().filter(c -> affColNames.contains(c.name())).collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override public Collection<Column> valueColumns() {
        return cols.values().stream().filter(c -> !keyColNames.contains(c.name())).collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override public String tableName() {
        return tableName;
    }

    /** {@inheritDoc} */
    @Override public String canonicalName() {
        return schemaName + '.' + tableName;
    }

    /** {@inheritDoc} */
    @Override public Collection<TableIndex> indices() {
        return Collections.unmodifiableCollection(indices.values());
    }

    /** {@inheritDoc} */
    @Override public TableModificationBuilder toBuilder() {
        return new TableModificationBuilderImpl(this);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "SchemaTable[" +
            "name='" + tableName + '\'' +
            ", keyCols=" + keyColNames +
            ", affCols=" + affColNames +
            ", column=" + cols.values() +
            ", indices=" + indices.values() +
            ']';
    }
}
