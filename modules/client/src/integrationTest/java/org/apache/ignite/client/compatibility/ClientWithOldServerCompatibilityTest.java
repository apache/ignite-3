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

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.compatibility.containers.IgniteServerContainer;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.table.Table;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests that current Java client can work with all older server versions.
 */
public class ClientWithOldServerCompatibilityTest {
    private static final String TABLE_NAME_TEST = "test";
    private static final String TABLE_NAME_ALL_COLUMNS = "all_columns";

    private static IgniteServerContainer serverContainer;
    private IgniteClient client;

    @BeforeAll
    static void beforeAll() throws Exception {
        // TODO: Parametrize the server version to test against multiple versions.
        serverContainer = new IgniteServerContainer("3.0.0");
        serverContainer.start();

        activateCluster(serverContainer.restPort());
        waitForActivation(serverContainer.clientPort());
    }

    @AfterAll
    static void afterAll() {
        if (serverContainer != null) {
            serverContainer.stop();
        }
    }

    @BeforeEach
    void beforeEach() {
        client = IgniteClient.builder()
                .addresses("localhost:" + serverContainer.clientPort())
                .build();
    }

    @AfterEach
    void afterEach() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    public void testClusterNodes() {
        assertThat(client.clusterNodes(), Matchers.hasSize(1));
    }

    @Test
    public void testTable() {
        createDefaultTables();

        Table testTable = client.tables().table(TABLE_NAME_TEST);
        assertNotNull(testTable);

        assertEquals(TABLE_NAME_TEST, testTable.name());
        assertEquals(TABLE_NAME_TEST, testTable.qualifiedName().objectName());
        assertEquals(TABLE_NAME_TEST, testTable.qualifiedName().schemaName());
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    public void testTables() {
        createDefaultTables();

        List<Table> tables = client.tables().tables();

        var testTable = tables.stream().filter(t -> t.name().equals(TABLE_NAME_TEST)).findFirst().get();

        assertEquals(TABLE_NAME_TEST, testTable.name());
        assertEquals(TABLE_NAME_TEST, testTable.qualifiedName().objectName());
        assertEquals(TABLE_NAME_TEST, testTable.qualifiedName().schemaName());
    }

    private void createDefaultTables() {
        createTable(TABLE_NAME_TEST);
        createTable(TABLE_NAME_ALL_COLUMNS); // TODO
    }

    private void createTable(String tableName) {
        String query = "CREATE TABLE IF NOT EXISTS " + tableName + " (id INT PRIMARY KEY, name VARCHAR)";
        try (var ignored = client.sql().execute(null, query)) { }
    }

    private static void activateCluster(int restPort) throws IOException {
        URL url = new URL("http://localhost:" + restPort + "/management/v1/cluster/init");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);

        String jsonInput = "{\"metaStorageNodes\": [\"defaultNode\"], \"clusterName\": \"myCluster\"}";

        try (OutputStream os = conn.getOutputStream()) {
            os.write(jsonInput.getBytes());
            os.flush();
        }

        int responseCode = conn.getResponseCode();
        System.out.println("Response Code: " + responseCode);
        conn.disconnect();
    }

    private static void waitForActivation(int clientPort) throws InterruptedException {
        IgniteTestUtils.waitForCondition(() -> {
            try (IgniteClient ignored = IgniteClient.builder()
                    .connectTimeout(500)
                    .addresses("localhost:" + clientPort)
                    .build()) {
                return true;
            } catch (Exception e) {
                return false;
            }
        }, 1_000, 20_000);
    }
}
