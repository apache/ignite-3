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

package org.apache.ignite.internal.runner.app.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests that check table names returned from table/tables operations.
 */
@SuppressWarnings("resource")
public class ItThinClientTablesGetListTest extends ItAbstractThinClientTest {

    private static final Set<QualifiedName> TABLE_NAMES = Set.of(
            QualifiedName.parse("public.oranges"),
            QualifiedName.parse("abc.oranges"),
            QualifiedName.parse("\"ABc\".oranges"),
            QualifiedName.parse("public.\"Apples\""),
            QualifiedName.parse("abc.\"Apples\""),
            QualifiedName.parse("\"ABc\".\"Apples\"")
    );

    @BeforeAll
    public void before() {
        Ignite server = server();

        for (Table table : server().tables().tables()) {
            try (ResultSet<?> rs = server.sql().execute("DROP TABLE " + table.qualifiedName().toCanonicalForm())) {
                assertNotNull(rs);
            }
        }

        server.sql().executeScript("CREATE SCHEMA abc;"
                + "CREATE SCHEMA \"ABc\";"
                + "CREATE TABLE oranges (id INT, val VARCHAR, PRIMARY KEY (id));"
                + "CREATE TABLE abc.oranges (id INT, val VARCHAR, PRIMARY KEY (id));"
                + "CREATE TABLE \"ABc\".oranges (id INT, val VARCHAR, PRIMARY KEY (id));"
                + "CREATE TABLE \"Apples\" (id INT, val VARCHAR, PRIMARY KEY (id));"
                + "CREATE TABLE abc.\"Apples\" (id INT, val VARCHAR, PRIMARY KEY (id));"
                + "CREATE TABLE \"ABc\".\"Apples\" (id INT, val VARCHAR, PRIMARY KEY (id));"
        );
    }

    @Test
    public void listTables() {
        IgniteClient client = client();
        Ignite server = server();

        // QualifiedName variant
        {
            Set<QualifiedName> clientTableNames = client.tables().tables()
                    .stream()
                    .map(Table::qualifiedName)
                    .collect(Collectors.toSet());

            Set<QualifiedName> serverTableNames = server.tables().tables()
                    .stream()
                    .map(Table::qualifiedName)
                    .collect(Collectors.toSet());

            assertEquals(TABLE_NAMES, clientTableNames);
            assertEquals(serverTableNames, clientTableNames);
        }

        // String variant
        {
            Set<String> clientTableNames = client.tables().tables()
                    .stream()
                    .map(Table::name)
                    .collect(Collectors.toSet());

            Set<String> serverTableNames = server.tables().tables()
                    .stream()
                    .map(Table::name)
                    .collect(Collectors.toSet());

            Set<String> stringNames = TABLE_NAMES.stream()
                    .map(QualifiedName::toCanonicalForm)
                    .collect(Collectors.toSet());

            assertEquals(stringNames, clientTableNames);
            assertEquals(serverTableNames, clientTableNames);
        }
    }

    @Test
    public void getTable() {
        for (QualifiedName name : TABLE_NAMES) {
            {
                // QualifiedName variant
                Table clientTable = client().tables().table(name);
                assertNotNull(clientTable, name.toCanonicalForm() + " does not exist");
            }
            {
                // String variant
                Table clientTable = client().tables().table(name.toCanonicalForm());
                assertNotNull(clientTable, name.toCanonicalForm() + " does not exist");
            }
        }

        Set<QualifiedName> notExistingNames = Set.of(
                QualifiedName.parse("public.\"oranges\""),
                QualifiedName.parse("abc.\"oranges\""),
                QualifiedName.parse("\"Abc\".\"oranges\""),
                QualifiedName.parse("public.apples"),
                QualifiedName.parse("abc.apples"),
                QualifiedName.parse("\"Abc\".apples")
        );

        for (QualifiedName name : notExistingNames) {
            {
                // QualifiedName variant
                Table clientTable = client().tables().table(name);
                assertNull(clientTable, name.toCanonicalForm() + " should not exist");
            }
            {
                // String variant
                Table clientTable = client().tables().table(name.toCanonicalForm());
                assertNull(clientTable, name.toCanonicalForm() + " should not exist");
            }
        }
    }

    @Test
    public void getTablesReturnsPublicSchemaTablesWhenSchemaIsNotSpecified() {
        for (QualifiedName name : Set.of(
                QualifiedName.parse("oranges"),
                QualifiedName.parse("\"Apples\"")
        )) {
            {
                // QualifiedName variant
                Table clientTable = client().tables().table(name);
                assertNotNull(clientTable, name.toCanonicalForm() + " does not exist");
                assertEquals("PUBLIC", clientTable.qualifiedName().schemaName());
            }
            {
                // String variant
                Table clientTable = client().tables().table(name.toCanonicalForm());
                assertNotNull(clientTable, name.toCanonicalForm() + " does not exist");
                assertEquals("PUBLIC", clientTable.qualifiedName().schemaName());
            }
        }
    }
}
