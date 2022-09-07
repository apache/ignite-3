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

package org.apache.ignite.internal.sql.engine.schema;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

/**
 * Ignite schema.
 */
public class IgniteSchema extends AbstractSchema {
    private final String schemaName;

    private final Map<String, Table> tblMap;

    private final Map<UUID, IgniteIndex> idxMap;

    /**
     * Creates a Schema.
     *
     * @param schemaName Schema name.
     */
    public IgniteSchema(String schemaName, Map<String, Table> tblMap, Map<UUID, IgniteIndex> idxMap) {
        this.schemaName = schemaName;
        this.tblMap = tblMap == null ? new ConcurrentHashMap<>() : new ConcurrentHashMap<>(tblMap);
        this.idxMap = idxMap == null ? new ConcurrentHashMap<>() : new ConcurrentHashMap<>(idxMap);
    }

    /**
     * Creates a Schema.
     *
     * @param schemaName Schema name.
     */
    public IgniteSchema(String schemaName) {
        this(schemaName, null, null);
    }

    public static IgniteSchema copy(IgniteSchema old) {
        return new IgniteSchema(old.schemaName, old.tblMap, old.idxMap);
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
     * @param tblName Table name.
     * @param tbl Table.
     */
    public void addTable(String tblName, InternalIgniteTable tbl) {
        tblMap.put(tblName, tbl);
    }

    /**
     * Remove table.
     *
     * @param tblName Table name.
     */
    public void removeTable(String tblName) {
        InternalIgniteTable rmv = (InternalIgniteTable) tblMap.remove(tblName);

        // TODO: https://issues.apache.org/jira/browse/IGNITE-17474
        // Decouple tables and indices.
        idxMap.values().removeIf(idx -> idx.index().tableId().equals(rmv.id()));
    }

    /**
     * Add index.
     *
     * @param indexId Index id.
     * @param index Index.
     */
    public void addIndex(UUID indexId, IgniteIndex index) {
        idxMap.put(indexId, index);
    }

    /**
     * Remove index.
     *
     * @param indexId Index id.
     * @return Removed index.
     */
    public IgniteIndex removeIndex(UUID indexId) {
        return idxMap.remove(indexId);
    }

    /**
     * Gets index by id.
     *
     * @param indexId Index id.
     * @return Index.
     */
    public IgniteIndex index(UUID indexId) {
        return idxMap.get(indexId);
    }
}
