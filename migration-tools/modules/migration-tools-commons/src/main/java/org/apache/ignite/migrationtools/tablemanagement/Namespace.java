/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
