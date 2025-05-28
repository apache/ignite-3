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

package org.apache.ignite.client.compatibility;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.Table;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Base class for client compatibility tests. Contains actual tests logic, without infrastructure initialization.
 */
public abstract class ClientCompatibilityTestBase {
    private static final String TABLE_NAME_TEST = "TEST";
    private static final String TABLE_NAME_ALL_COLUMNS = "ALL_COLUMNS";

    IgniteClient client;

    @Test
    public void testClusterNodes() {
        assertThat(client.clusterNodes(), Matchers.hasSize(1));
    }

    @Test
    @Disabled("IGNITE-25514")
    public void testTableByName() {
        createDefaultTables();

        Table testTable = client.tables().table(TABLE_NAME_TEST);
        assertNotNull(testTable);

        assertEquals(TABLE_NAME_TEST, testTable.qualifiedName().objectName());
    }

    @Test
    public void testTableByQualifiedName() {
        createDefaultTables();

        Table testTable = client.tables().table(QualifiedName.fromSimple(TABLE_NAME_TEST));
        assertNotNull(testTable);

        assertEquals(TABLE_NAME_TEST, testTable.qualifiedName().objectName());
    }

    @Test
    public void testTables() {
        createDefaultTables();

        List<Table> tables = client.tables().tables();

        List<String> tableNames = tables.stream()
                .map(t -> t.qualifiedName().objectName())
                .collect(Collectors.toList());

        assertThat(tableNames, Matchers.containsInAnyOrder(TABLE_NAME_TEST, TABLE_NAME_ALL_COLUMNS));
    }

    private void createDefaultTables() {
        createTable(TABLE_NAME_TEST);
        createTable(TABLE_NAME_ALL_COLUMNS); // TODO
    }

    private void createTable(String tableName) {
        String query = "CREATE TABLE IF NOT EXISTS " + tableName + " (id INT PRIMARY KEY, name VARCHAR)";
        try (var ignored = client.sql().execute(null, query)) { }
    }
}
