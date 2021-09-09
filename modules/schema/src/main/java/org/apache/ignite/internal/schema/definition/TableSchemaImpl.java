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

package org.apache.ignite.internal.schema.definition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.internal.schema.modification.TableModificationBuilderImpl;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.schema.definition.Column;
import org.apache.ignite.schema.definition.PrimaryKey;
import org.apache.ignite.schema.definition.TableSchema;
import org.apache.ignite.schema.definition.index.Index;
import org.apache.ignite.schema.modification.TableModificationBuilder;

/**
 * Table.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class TableSchemaImpl extends AbstractSchemaObject implements TableSchema {
    /** Schema name. */
    private final String schemaName;

    /** Key columns. */
    private final LinkedHashMap<String, Column> cols;

    /** Indices. */
    private final Map<String, Index> indices;

    /** Cached key columns. */
    private final List<Column> keyCols;

    /** Cached key affinity columns. */
    private final List<Column> affCols;

    /** Cached value columns. */
    private final List<Column> valCols;

    /**
     * Constructor.
     *
     * @param schemaName Schema name.
     * @param tableName Table name.
     * @param cols Columns.
     * @param primaryKey Primary key.
     * @param indices Indices.
     */
    public TableSchemaImpl(
        String schemaName,
        String tableName,
        LinkedHashMap<String, Column> cols,
        PrimaryKey primaryKey,
        Map<String, Index> indices
    ) {
        super(tableName);

        this.schemaName = schemaName;
        this.cols = cols;
        this.indices = indices;

        Set<String> pkCols = primaryKey.columns();
        Set<String> pkAffCols = primaryKey.affinityColumns();

        keyCols = new ArrayList<>(pkCols.size());
        affCols = new ArrayList<>(pkAffCols.size());
        valCols = new ArrayList<>(cols.size() - pkCols.size());

        for (Map.Entry<String, Column> e : cols.entrySet()) {
            if (pkCols.contains(e.getKey())) {
                keyCols.add(e.getValue());

                if (pkAffCols.contains(e.getValue().name()))
                    affCols.add(e.getValue());
            } else
                valCols.add(e.getValue());
        }
    }

    /** {@inheritDoc} */
    @Override public String schemaName() {
        return schemaName;
    }

    /** {@inheritDoc} */
    @Override public Collection<Column> keyColumns() {
        return keyCols;
    }

    /** {@inheritDoc} */
    @Override public Collection<Column> affinityColumns() {
        return affCols;
    }

    /** {@inheritDoc} */
    @Override public Collection<Column> valueColumns() {
        return valCols;
    }

    /** {@inheritDoc} */
    @Override public Collection<Index> indices() {
        return Collections.unmodifiableCollection(indices.values());
    }

    /** {@inheritDoc} */
    @Override public TableModificationBuilder toBuilder() {
        return new TableModificationBuilderImpl(this);
    }

    /**
     * @param name Column name.
     * @return {@code True} if column with given name already exists, {@code false} otherwise.
     */
    public boolean hasColumn(String name) {
        return cols.containsKey(name);
    }

    /**
     * @param name Column name.
     * @return {@code True} if key column with given name already exists, {@code false} otherwise.
     */
    public boolean hasKeyColumn(String name) {
        return keyCols.stream().anyMatch(c -> c.name().equals(name));
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TableSchemaImpl.class, this);
    }
}
