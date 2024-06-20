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

package org.apache.ignite.example.tx;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.tx.IgniteTransactions;

/**
 * This example demonstrates the usage of the {@link IgniteTransactions} API.
 *
 * <p>To run the example, please do the following:
 * <ol>
 *     <li>Open the Ignite 3 project in your IDE of choice.</li>
 *     <li>
 *         Download the Ignite 3 ZIP packaging with DB and CLI parts.
 *         Or build them from the Ignite 3 sources (see {@code DEVNOTES.md}).
 *         Unpack.
 *     </li>
 *     <li>
 *         Prepare the environment variables:<br>
 *         <code>
 *             export IGNITE_HOME=/path/to/ignite3-db-VERSION<br>
 *             export IGNITE_CLI_HOME=/path/to/ignite3-cli-VERSION<br>
 *             export IGNITE_SRC_HOME=/path/to/ignite/sources
 *         </code>
 *     </li>
 *     <li>
 *         Override the default configuration file:<br>
 *         {@code echo "CONFIG_FILE=$IGNITE_SRC_HOME/examples/config/ignite-config.conf" >> $IGNITE_HOME/etc/vars.env}
 *     </li>
 *     <li>
 *         Start an Ignite node using the startup script from the DB part:<br>
 *         {@code ${IGNITE_HOME}/bin/ignite3db start}
 *     </li>
 *     <li>
 *         Initialize the cluster using Ignite 3 CLI from the CLI part:<br>
 *         {@code $IGNITE_CLI_HOME/bin/ignite3 cluster init --name myCluster1 --metastorage-group defaultNode
 *         --cluster-management-group defaultNode}
 *     </li>
 *     <li>Run the example from the IDE.</li>
 *     <li>
 *         Stop the Ignite node using the startup script:<br>
 *         {@code ${IGNITE_HOME}/bin/ignite3db stop}
 *     </li>
 * </ol>
 */
public class TransactionsExample {
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
                    "CREATE TABLE accounts ("
                            + "accountNumber INT PRIMARY KEY,"
                            + "firstName     VARCHAR,"
                            + "lastName      VARCHAR,"
                            + "balance       DOUBLE)"
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
            //--------------------------------------------------------------------------------------
            //
            // Creating an account.
            //
            //--------------------------------------------------------------------------------------

            KeyValueView<AccountKey, Account> accounts = client.tables()
                    .table("accounts")
                    .keyValueView(AccountKey.class, Account.class);

            final AccountKey key = new AccountKey(123);

            accounts.put(null, key, new Account("John", "Doe", 1000.0d));

            System.out.println("\nInitial balance: " + accounts.get(null, key).balance);

            //--------------------------------------------------------------------------------------
            //
            // Using synchronous transactional API to update the balance.
            //
            //--------------------------------------------------------------------------------------

            client.transactions().runInTransaction(tx -> {
                Account account = accounts.get(tx, key);

                account.balance += 200.0d;

                accounts.put(tx, key, account);
            });

            System.out.println("\nBalance after the sync transaction: " + accounts.get(null, key).balance);

            //--------------------------------------------------------------------------------------
            //
            // Using asynchronous transactional API to update the balance.
            //
            //--------------------------------------------------------------------------------------

            CompletableFuture<Void> fut = client.transactions().beginAsync().thenCompose(tx ->
                    accounts
                        .getAsync(tx, key)
                        .thenCompose(account -> {
                            account.balance += 300.0d;

                            return accounts.putAsync(tx, key, account);
                        })
                        .thenCompose(ignored -> tx.commitAsync())
            );

            // Wait for completion.
            fut.join();

            System.out.println("\nBalance after the async transaction: " + accounts.get(null, key).balance);
        } finally {
            System.out.println("\nDropping the table...");

            try (
                    Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800/");
                    Statement stmt = conn.createStatement()
            ) {
                stmt.executeUpdate("DROP TABLE accounts");
            }
        }
    }

    /**
     * POJO class that represents key.
     */
    static class AccountKey {
        int accountNumber;

        /**
         * Default constructor (required for deserialization).
         */
        @SuppressWarnings("unused")
        public AccountKey() {
        }

        public AccountKey(int accountNumber) {
            this.accountNumber = accountNumber;
        }
    }

    /**
     * POJO class that represents value.
     */
    static class Account {
        String firstName;
        String lastName;
        double balance;

        /**
         * Default constructor (required for deserialization).
         */
        @SuppressWarnings("unused")
        public Account() {
        }

        public Account(String firstName, String lastName, double balance) {
            this.firstName = firstName;
            this.lastName = lastName;
            this.balance = balance;
        }
    }
}
