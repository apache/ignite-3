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

package org.apache.ignite.migrationtools.tablemanagement;

/** Old class to help navigate the limitation of custom schemas in Ignite 3. */
public class Namespace {
    private static String MIGRATION_TOOLS_SCHEMA = "MIGRATION_TOOLS_PVT";

    /**
     * Resolves the full table name for a table inside our namespace.
     *
     * @param tableName Unquoted table name.
     * @return The full unquoted full table name (with the namespace prefix).
     */
    public static String resolveTableName(String tableName) {
        return MIGRATION_TOOLS_SCHEMA + "_" + tableName;
    }

    /**
     * Check if the provided table name is inside our namespace.
     *
     * @param tableName Name of the table to check. May be quoted or unquoted.
     * @return Whether the table is inside our namespace or not.
     */
    public static boolean isTableFromNamespace(String tableName) {
        if (tableName.charAt(0) == '"') {
            tableName = tableName.substring(1);
        }

        return tableName.startsWith(MIGRATION_TOOLS_SCHEMA + "_");
    }

    private Namespace() {
        // Intentionally left blank.
    }
}
