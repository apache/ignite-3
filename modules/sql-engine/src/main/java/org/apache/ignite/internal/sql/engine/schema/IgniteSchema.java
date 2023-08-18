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

package org.apache.ignite.internal.sql.engine.schema;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite schema.
 */
public class IgniteSchema extends AbstractSchema {
    static final long INITIAL_VERSION = -1;

    private final String schemaName;

    private final Map<String, IgniteTable> tableByName;
    private final Map<Integer, IgniteTable> tableById;

    private final Map<Integer, IgniteIndex> idxMap;

    private final long schemaVersion;

    /**
     * Creates a Schema with given tables and indexes.
     *
     * @param schemaName A name of the schema to create.
     * @param tableMap A collection of a tables belonging to the schema.
     * @param indexMap A collection of an indexes belonging to the schema.
     */
    public IgniteSchema(
            String schemaName,
            @Nullable Map<String, IgniteTable> tableMap,
            @Nullable Map<Integer, IgniteIndex> indexMap,
            long schemaVersion
    ) {
        this.schemaName = schemaName;
        this.tableByName = tableMap == null ? new ConcurrentHashMap<>() : new ConcurrentHashMap<>(tableMap);
        this.idxMap = indexMap == null ? new ConcurrentHashMap<>() : new ConcurrentHashMap<>(indexMap);
        this.schemaVersion = schemaVersion;

        this.tableById = tableMap == null ? new ConcurrentHashMap<>() :
                tableMap.values().stream().collect(Collectors.toConcurrentMap(IgniteTable::id, Function.identity()));
    }

    /**
     * Creates an empty Schema.
     *
     * @param schemaName A name of the schema to create.
     */
    public IgniteSchema(String schemaName) {
        this(schemaName, null, null, INITIAL_VERSION);
    }

    /**
     * Creates an empty Schema.
     *
     * @param schemaName A name of the schema to create.
     */
    public IgniteSchema(String schemaName, long schemaVersion) {
        this(schemaName, null, null, schemaVersion);
    }

    public static IgniteSchema copy(IgniteSchema old, long schemaVersion) {
        return new IgniteSchema(old.schemaName, old.tableByName, old.idxMap, schemaVersion);
    }

    /**
     * Get schema name.
     *
     * @return Schema name.
     */
    public String getName() {
        return schemaName;
    }

    /** {@inheritDoc} */
    @Override
    protected Map<String, Table> getTableMap() {
        return Collections.unmodifiableMap(tableByName);
    }

    /**
     * Return table by given id.
     */
    public IgniteTable getTable(int tableId) {
        return tableById.get(tableId);
    }

    /**
     * Add table.
     *
     * @param tbl Table.
     */
    public void addTable(IgniteTable tbl) {
        tableByName.put(tbl.name(), tbl);
        tableById.put(tbl.id(), tbl);
    }

    /**
     * Remove table.
     *
     * @param tblName Table name.
     */
    public void removeTable(String tblName) {
        tableByName.remove(tblName);
    }

    /**
     * Add index.
     *
     * @param indexId Index id.
     * @param index Index.
     */
    public void addIndex(int indexId, IgniteIndex index) {
        idxMap.put(indexId, index);
    }

    /**
     * Remove index.
     *
     * @param indexId Index id.
     * @return Removed index.
     */
    public IgniteIndex removeIndex(int indexId) {
        return idxMap.remove(indexId);
    }

    /**
     * Gets index by id.
     *
     * @param indexId Index id.
     * @return Index.
     */
    public IgniteIndex index(int indexId) {
        return idxMap.get(indexId);
    }

    /**
     * Return actual schema version.
     */
    public long schemaVersion() {
        return schemaVersion;
    }
}
