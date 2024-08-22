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

package org.apache.ignite.internal.runner.app;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.function.Executable;

/**
 * Ignition interface tests.
 */
abstract class AbstractSchemaChangeTest extends ClusterPerTestIntegrationTest {
    /** Table name. */
    public static final String TABLE = "TBL1";

    /**
     * Returns grid nodes.
     */
    List<Ignite> startGrid() {
        return cluster.runningNodes().collect(toList());
    }

    /**
     * Creates tables.
     *
     */
    protected void createTable() {
        executeSql("CREATE TABLE tbl1(key BIGINT PRIMARY KEY, valint INT, valblob BINARY,"
                + "valdecimal DECIMAL, valbigint BIGINT, valstr VARCHAR NOT NULL DEFAULT 'default')");
    }

    /**
     * Adds column.
     *
     * @param columnToAdd Column to add.
     */
    protected void addColumn(String columnToAdd) {
        executeSql("ALTER TABLE " + TABLE + " ADD COLUMN " + columnToAdd);
    }

    /**
     * Drops column.
     *
     * @param colName Name of column to drop.
     */
    protected void dropColumn(String colName) {
        executeSql("ALTER TABLE " + TABLE + " DROP COLUMN " + colName);
    }

    /**
     * Renames column.
     *
     * @param oldName Old column name.
     * @param newName New column name.
     */
    // TODO: IGNITE-20315 syntax may change
    protected void renameColumn(String oldName, String newName) {
        executeSql(String.format("ALTER TABLE %s RENAME COLUMN %s TO %s", TABLE, oldName, newName));
    }

    /**
     * Changes column default.
     *
     * @param colName Column name.
     * @param def Default value.
     */
    protected void changeDefault(String colName, String def) {
        executeSql(String.format("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT '%s'", TABLE, colName, def));
    }

    protected static <T extends Throwable> void assertThrowsWithCause(Class<T> expectedType, Executable executable) {
        Throwable ex = assertThrows(IgniteException.class, executable);

        while (ex.getCause() != null) {
            if (expectedType.isInstance(ex.getCause())) {
                return;
            }

            ex = ex.getCause();
        }

        fail("Expected cause wasn't found.");
    }
}
