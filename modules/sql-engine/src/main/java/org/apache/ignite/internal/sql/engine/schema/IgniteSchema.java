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
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite schema.
 */
public class IgniteSchema extends AbstractSchema {
    static final int INITIAL_VERSION = -1;

    private final String schemaName;

    private final Map<String, Table> tblMap;

    private final Map<Integer, IgniteIndex> idxMap;

    private final int schemaVersion;

    /**
     * Creates a Schema with given tables and indexes.
     *
     * @param schemaName A name of the schema to create.
     * @param tableMap A collection of a tables belonging to the schema.
     * @param indexMap A collection of an indexes belonging to the schema.
     */
    public IgniteSchema(
            String schemaName,
            @Nullable Map<String, Table> tableMap,
            @Nullable Map<Integer, IgniteIndex> indexMap,
            int schemaVersion
    ) {
        this.schemaName = schemaName;
        this.tblMap = tableMap == null ? new ConcurrentHashMap<>() : new ConcurrentHashMap<>(tableMap);
        this.idxMap = indexMap == null ? new ConcurrentHashMap<>() : new ConcurrentHashMap<>(indexMap);
        this.schemaVersion = schemaVersion;
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
    public IgniteSchema(String schemaName, int schemaVersion) {
        this(schemaName, null, null, schemaVersion);
    }

    public static IgniteSchema copy(IgniteSchema old, int schemaVersion) {
        return new IgniteSchema(old.schemaName, old.tblMap, old.idxMap, schemaVersion);
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
        return Collections.unmodifiableMap(tblMap);
    }

    /**
     * Add table.
     *
     * @param tbl Table.
     */
    public void addTable(IgniteTable tbl) {
        tblMap.put(tbl.name(), tbl);
    }

    /**
     * Remove table.
     *
     * @param tblName Table name.
     */
    public void removeTable(String tblName) {
        tblMap.remove(tblName);
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
    public int schemaVersion() {
        return schemaVersion;
    }
}
