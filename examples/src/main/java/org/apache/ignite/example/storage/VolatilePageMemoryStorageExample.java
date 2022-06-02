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

package org.apache.ignite.example.storage;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * This example demonstrates a usage of the PageMemory storage engine configured with an in-memory data region.
 *
 * <p>To run the example, do the following:
 * <ol>
 *     <li>Import the examples project into you IDE.</li>
 *     <li>
 *         Download and prepare artifacts for running an Ignite node using the CLI tool (if not done yet):<br>
 *         {@code ignite init}
 *     </li>
 *     <li>
 *         Start a server node using the CLI tool:<br>
 *         {@code ignite node start --config=$IGNITE_HOME/examples/config/ignite-config.json my-first-node}
 *     </li>
 *     <li>
 *         Cluster initialization using the CLI tool (if not done yet):<br>
 *         {@code ignite cluster init --cluster-name=ignite-cluster --node-endpoint=localhost:10300 --meta-storage-node=my-first-node}
 *     </li>
 *     <li>
 *         Add configuration for in-memory data region of of the PageMemory storage engine using the CLI tool (if not done yet):<br>
 *         {@code ignite config set --type=cluster "pageMemory.regions.in-memory:{persistent=false}"}
 *     </li>
 *     <li>Run the example in the IDE.</li>
 *     <li>
 *         Stop a server node using the CLI tool:<br>
 *         {@code ignite node stop my-first-node}
 *     </li>
 * </ol>
 */
public class VolatilePageMemoryStorageExample {
    /**
     * Main method of the example.
     *
     * @param args The command line arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        //--------------------------------------------------------------------------------------
        //
        // Creating a JDBC connection to connect to the cluster.
        //
        //--------------------------------------------------------------------------------------

        System.out.println("\nConnecting to server...");

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800/")) {
            //--------------------------------------------------------------------------------------
            //
            // Creating table.
            //
            //--------------------------------------------------------------------------------------

            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(
                        "CREATE TABLE ACCOUNTS ( "
                                + "ACCOUNT_ID INT PRIMARY KEY,"
                                + "FIRST_NAME VARCHAR, "
                                + "LAST_NAME  VARCHAR, "
                                + "BALANCE    DOUBLE) "
                                + "ENGINE pagememory "
                                + "WITH dataRegion='in-memory'"
                );
            }

            //--------------------------------------------------------------------------------------
            //
            // Populating 'ACCOUNTS' table.
            //
            //--------------------------------------------------------------------------------------

            System.out.println("\nPopulating 'ACCOUNTS' table...");

            try (PreparedStatement stmt = conn.prepareStatement(
                    "INSERT INTO ACCOUNTS (ACCOUNT_ID, FIRST_NAME, LAST_NAME, BALANCE) values (?, ?, ?, ?)"
            )) {
                stmt.setInt(1, 1);
                stmt.setString(2, "John");
                stmt.setString(3, "Doe");
                stmt.setDouble(4, 1000.0d);
                stmt.executeUpdate();

                stmt.setInt(1, 2);
                stmt.setString(2, "Jane");
                stmt.setString(3, "Roe");
                stmt.setDouble(4, 2000.0d);
                stmt.executeUpdate();

                stmt.setInt(1, 3);
                stmt.setString(2, "Mary");
                stmt.setString(3, "Major");
                stmt.setDouble(4, 1500.0d);
                stmt.executeUpdate();

                stmt.setInt(1, 4);
                stmt.setString(2, "Richard");
                stmt.setString(3, "Miles");
                stmt.setDouble(4, 1450.0d);
                stmt.executeUpdate();
            }

            //--------------------------------------------------------------------------------------
            //
            // Requesting information about all account owners.
            //
            //--------------------------------------------------------------------------------------

            System.out.println("\nAll accounts:");

            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery(
                        "SELECT ACCOUNT_ID, FIRST_NAME, LAST_NAME, BALANCE FROM ACCOUNTS ORDER BY ACCOUNT_ID"
                )) {
                    while (rs.next()) {
                        System.out.println("    "
                                + rs.getString(1) + ", "
                                + rs.getString(2) + ", "
                                + rs.getString(3) + ", "
                                + rs.getString(4));
                    }
                }
            }
        } finally {
            System.out.println("\nDropping the table...");

            try (
                    Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800/");
                    Statement stmt = conn.createStatement()
            ) {
                stmt.executeUpdate("DROP TABLE ACCOUNTS");
            }
        }
    }
}
