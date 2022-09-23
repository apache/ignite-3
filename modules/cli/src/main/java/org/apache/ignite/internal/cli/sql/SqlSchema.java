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

package org.apache.ignite.internal.cli.sql;

import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Sql schema representation.
 */
public class SqlSchema {
    private final Map<String, Map<String, Set<String>>> schema;

    public SqlSchema(Map<String, Map<String, Set<String>>> schema) {
        this.schema = schema;
    }


    public Set<String> schemas() {
        return Collections.unmodifiableSet(schema.keySet());
    }

    public Set<String> tables(String schemaName) {
        return Collections.unmodifiableSet(schema.getOrDefault(schemaName, Collections.emptyMap()).keySet());
    }

    /**
     * Retrieves column names and stores it in the schema cache.
     *
     * @param tableName name of the table.
     * @return set of column names.
     */
    public Set<String> getColumnNames(String tableName) {
        Entry<String, Map<String, Set<String>>> schema = findSchema(tableName);
        if (schema != null) {
            Map<String, Set<String>> tables = schema.getValue();
            return tables.computeIfAbsent(tableName, key -> getColumns(schema.getKey(), key));
        }
        return Collections.emptySet();
    }

    /**
     * Retrieves column names from the metadata.
     *
     * @param schemaName name of the schema.
     * @param tableName name of the table.
     * @return set of column names.
     */
    private Set<String> getColumns(String schemaName, String tableName) {
        Map<String, Set<String>> tables = schema.get(schemaName);
        if (tables != null) {
            return tables.getOrDefault(tableName, Collections.emptySet());
        }
        return Collections.emptySet();
    }

    /**
     * Finds table schema which contains a table with the given name.
     *
     * @param tableName name of the table to find.
     * @return map entry for the schema or null if table is not found.
     */
    private Entry<String, Map<String, Set<String>>> findSchema(String tableName) {
        for (Entry<String, Map<String, Set<String>>> entry : schema.entrySet()) {
            if (entry.getValue().containsKey(tableName)) {
                return entry;
            }
        }
        return null;
    }
}
