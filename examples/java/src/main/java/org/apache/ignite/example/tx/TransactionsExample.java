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

import static java.sql.DriverManager.getConnection;

import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.KeyValueView;

/* This example demonstrates the usage of the Ignite Transactions API */
public class TransactionsExample {

    public static void main(String[] args) throws Exception {

        /* Create 'accounts' table via JDBC */
        try (
                Connection conn = getConnection("jdbc:ignite:thin://127.0.0.1:10800/");
                Statement stmt = conn.createStatement()
        ) {
            stmt.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS accounts ("
                            + "accountNumber INT PRIMARY KEY,"
                            + "firstName VARCHAR,"
                            + "lastName VARCHAR,"
                            + "balance DOUBLE)"
            );
        }

        /* Creating a client to connect to the cluster */
        System.out.println("\nConnecting to server...");

        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()) {

            /* Prepare key-value view */
            KeyValueView<AccountKey, Account> accounts = client.tables()
                    .table("accounts")
                    .keyValueView(AccountKey.class, Account.class);

            AccountKey key = new AccountKey(123);

            /* Insert initial account */
            accounts.put(null, key, new Account("John", "Doe", 1000.0d));
            System.out.println("Initial balance: " + accounts.get(key).balance);

            /* Using synchronous transactional API to update the balance */
            client.transactions().runInTransaction(tx -> {
                Account acct = accounts.get(tx, key);
                acct.balance += 200.0d;
                accounts.put(tx, key, acct);
            });

            System.out.println("Balance after the sync transaction: " + accounts.get(key).balance);

            /* Using asynchronous transactional API to update the balance */
            CompletableFuture<Void> future = client.transactions().runInTransactionAsync(tx ->
                    accounts.getAsync(tx, key)
                            .thenCompose(acct -> {
                                acct.balance += 300.0d;
                                return accounts.putAsync(tx, key, acct);
                            })
            );
            future.join();
            System.out.println("Balance after the async transaction: " + accounts.get(key).balance);

        } finally {

            /* Drop table */
            System.out.println("\nDropping the table...");
            try (
                    Connection conn = getConnection("jdbc:ignite:thin://127.0.0.1:10800/");
                    Statement stmt = conn.createStatement()
            ) {
                stmt.executeUpdate("DROP TABLE IF EXISTS accounts");
            }
        }
    }

    /* POJO class for key */
    static class AccountKey {
        int accountNumber;

        /* Default constructor required for deserialization */
        AccountKey() {
        }

        AccountKey(int accountNumber) {
            this.accountNumber = accountNumber;
        }
    }

    /* POJO class for value */
    static class Account {
        String firstName;
        String lastName;
        double balance;

        /* Default constructor required for deserialization */
        Account() {
        }

        Account(String firstName, String lastName, double balance) {
            this.firstName = firstName;
            this.lastName = lastName;
            this.balance = balance;
        }
    }
}
