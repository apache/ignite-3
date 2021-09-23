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

package org.apache.ignite.sql.jdbc;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import org.apache.ignite.app.Ignite;
import org.apache.ignite.app.IgnitionManager;
import org.apache.ignite.jdbc.IgniteJdbcDriver;

/**
 * This example demonstrates usage of Ignite JDBC driver.
 * <p>
 * To run the example, do the following:
 * <ol>
 *     <li>Import the examples project into you IDE.</li>
 *     <li>Run the example in the IDE.</li>
 * </ol>
 */
public class SqlJdbcExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws Exception If example execution failed.
     */
    public static void main(String[] args) throws Exception {
        IgniteJdbcDriver.register();

        Ignite ignite = IgnitionManager.start(
            "node-0",
            Files.readString(Path.of( "examples/config/ignite-config.json").toAbsolutePath()),
            Path.of("work")
        );

        print("JDBC example started.");

        // Open JDBC connection
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.1.1:10800/")) {
            print("Connected to server.");

            try (Statement stmt = conn.createStatement()) {
                //---------------------------------------------------------------------------------
                //
                // Creating City table.
                //
                //     CREATE TABLE city (
                //         id       INT PRIMARY KEY,
                //         name     VARCHAR
                //     )
                //
                //---------------------------------------------------------------------------------
                stmt.executeUpdate("CREATE TABLE city (id INT PRIMARY KEY, name VARCHAR)");

                //---------------------------------------------------------------------------------
                //
                // Creating accounts table.
                //
                //     CREATE TABLE accounts (
                //         accountNumber INT PRIMARY KEY,
                //         cityId        INT,
                //         firstName     VARCHAR,
                //         lastName      VARCHAR,
                //         balance       DOUBLE
                //     )
                //
                //---------------------------------------------------------------------------------
                stmt.executeUpdate("CREATE TABLE accounts (accountNumber INT PRIMARY KEY, cityId INT, " +
                    "firstName VARCHAR, lastName VARCHAR, balance DOUBLE)");
            }

            print("Created database objects.");

            // Populate City table with PreparedStatement.
            try (PreparedStatement stmt = conn.prepareStatement("INSERT INTO city (id, name) VALUES (?, ?)")) {
                stmt.setInt(1, 1);
                stmt.setString(2, "Forest Hill");
                stmt.executeUpdate();

                stmt.setInt(1, 2);
                stmt.setString(2, "Denver");
                stmt.executeUpdate();

                stmt.setInt(1, 3);
                stmt.setString(2, "St. Petersburg");
                stmt.executeUpdate();
            }

            // Populate Person table with PreparedStatement.
            try (PreparedStatement stmt =
                conn.prepareStatement("INSERT INTO accounts (accountNumber, cityId, firstName, lastName, balance)" +
                    " values (?, ?, ?, ?, ?)")) {
                stmt.setInt(1, 1);
                stmt.setInt(2, 1);
                stmt.setString(3, "John");
                stmt.setString(4, "Doe");
                stmt.setDouble(5, 1000.0d);
                stmt.executeUpdate();

                stmt.setInt(1, 2);
                stmt.setInt(2, 1);
                stmt.setString(3, "Jane");
                stmt.setString(4, "Roe");
                stmt.setDouble(5, 2000.0d);
                stmt.executeUpdate();

                stmt.setInt(1, 3);
                stmt.setInt(2, 2);
                stmt.setString(3, "Mary");
                stmt.setString(4, "Major");
                stmt.setDouble(5, 1500.0d);
                stmt.executeUpdate();

                stmt.setInt(1, 4);
                stmt.setInt(2, 3);
                stmt.setString(3, "Richard");
                stmt.setString(4, "Miles");
                stmt.setDouble(5, 1450.0d);
                stmt.executeUpdate();
            }

            print("Populated data.");

            //---------------------------------------------------------------------------------
            //
            // Gets accounts joined with cities.
            //
            //---------------------------------------------------------------------------------

            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs =
                    stmt.executeQuery("SELECT a.firstName, a.secondName, c.name FROM accounts a " +
                        "INNER JOIN city c on c.id = a.cityId")) {
                    print("Query results:");

                    while (rs.next())
                        System.out.println(">>>    " + rs.getString(1) + ", " + rs.getString(2)
                            + ", " + rs.getString(3));
                }
            }

            //---------------------------------------------------------------------------------
            //
            // Gets accounts with balance lower than 1500.
            //
            //---------------------------------------------------------------------------------

            print("Accounts with low balance.");

            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs =
                         stmt.executeQuery("SELECT a.firstName, a.secondName, a.balance FROM accounts a " +
                             "WHERE a.balance < 1500.0")) {
                    print("Query results:");

                    while (rs.next())
                        System.out.println(">>>    " + rs.getString(1) + ", " + rs.getString(2)
                            + ", " + rs.getDouble(3));
                }
            }

            //---------------------------------------------------------------------------------
            //
            // Delete one of accounts.
            //
            //---------------------------------------------------------------------------------

            try (PreparedStatement stmt = conn.prepareStatement("DELETE FROM person WHERE id = ?")) {
                stmt.setLong(1, 1L);
                stmt.executeUpdate();
            }

            print("Existing accounts.");

            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs =
                         stmt.executeQuery("SELECT a.firstName, a.secondName, c.name FROM accounts a " +
                             "INNER JOIN city c on c.id = a.cityId")) {
                    print("Query results:");

                    while (rs.next())
                        System.out.println(">>>    " + rs.getString(1) + ", " + rs.getString(2)
                            + ", " + rs.getString(3));
                }
            }

            // Drop database objects.
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("DROP TABLE accounts");
                stmt.executeUpdate("DROP TABLE city");
            }

            print("Dropped database objects.");
        }

        print("JDBC example finished.");

        ignite.close();
    }

    /**
     * Prints message.
     *
     * @param msg Message to print before all objects are printed.
     */
    private static void print(String msg) {
        System.out.println();
        System.out.println(">>> " + msg);
    }
}
