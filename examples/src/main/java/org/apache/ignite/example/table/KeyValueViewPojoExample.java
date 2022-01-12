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

package org.apache.ignite.example.table;

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.KeyValueView;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * This example demonstrates the usage of the {@link KeyValueView} API.
 *
 * <p>To run the example, do the following:
 * <ol>
 *     <li>Import the examples project into you IDE.</li>
 *     <li>
 *         Start a server node using the CLI tool:<br>
 *         {@code ignite node start --config=$IGNITE_HOME/examples/config/ignite-config.json my-first-node}
 *     </li>
 *     <li>Run the example in the IDE.</li>
 * </ol>
 */
public class KeyValueViewPojoExample {
    /**
     * Main method of the example.
     *
     * @param args The command line arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        //--------------------------------------------------------------------------------------
        //
        // Creating 'accounts' table.
        //
        //--------------------------------------------------------------------------------------

        try (
            Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800/");
            Statement stmt = conn.createStatement()
        ) {
            stmt.executeUpdate(
                "CREATE TABLE accounts (" +
                "    accountNumber INT PRIMARY KEY," +
                "    firstName     VARCHAR," +
                "    lastName      VARCHAR," +
                "    balance       DOUBLE" +
                ")"
            );
        }

        //--------------------------------------------------------------------------------------
        //
        // Creating a client to connect to the cluster.
        //
        //--------------------------------------------------------------------------------------

        System.out.println("\nConnecting to server...");

        try (IgniteClient client = IgniteClient.builder()
            .addresses("127.0.0.1:10800")
            .build()
        ) {
            class AccountKey {
                final int accountNumber;

                public AccountKey(int accountNumber) {
                    this.accountNumber = accountNumber;
                }
            }

            class Account {
                final String firstName;
                final String lastName;
                final double balance;

                public Account(String firstName, String lastName, double balance) {
                    this.firstName = firstName;
                    this.lastName = lastName;
                    this.balance = balance;
                }
            }

            //--------------------------------------------------------------------------------------
            //
            // Creating a key-value view for the 'accounts' table.
            //
            //--------------------------------------------------------------------------------------

            KeyValueView<AccountKey, Account> kvView = client.tables()
                .table("PUBLIC.accounts")
                .keyValueView(AccountKey.class, Account.class);

            //--------------------------------------------------------------------------------------
            //
            // Performing the 'put' operation.
            //
            //--------------------------------------------------------------------------------------

            System.out.println("\nInserting a key-value pair into the 'accounts' table...");

            AccountKey key = new AccountKey(123456);

            Account value = new Account(
                "Val",
                "Kulichenko",
                100.00d
            );

            kvView.put(null, key, value);

            //--------------------------------------------------------------------------------------
            //
            // Performing the 'get' operation.
            //
            //--------------------------------------------------------------------------------------

            System.out.println("\nRetrieving a value using KeyValueView API...");

            value = kvView.get(null, key);

            System.out.println(
                "\nRetrieved value:\n" +
                "    Account Number: " + key.accountNumber + '\n' +
                "    Owner: " + value.firstName + " " + value.lastName + '\n' +
                "    Balance: $" + value.balance);
        }

        System.out.println("\nDropping the table...");

        try (
            Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800/");
            Statement stmt = conn.createStatement()
        ) {
            stmt.executeUpdate("DROP TABLE accounts");
        }
    }
}
