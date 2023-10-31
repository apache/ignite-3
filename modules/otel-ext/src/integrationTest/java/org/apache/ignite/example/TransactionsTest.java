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

package org.apache.ignite.example;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.tx.IgniteTransactions;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.Test;

/**
 * This example demonstrates the usage of the {@link IgniteTransactions} API.
 */
public class TransactionsTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testPutConcurrently() throws Exception {
        Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800/");
        Statement stmt = conn.createStatement();

        try {
            stmt.executeUpdate(
                    "CREATE TABLE accounts ("
                            + "accountNumber INT PRIMARY KEY,"
                            + "firstName     VARCHAR,"
                            + "lastName      VARCHAR,"
                            + "balance       DOUBLE)"
            );

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

                AccountKey key = new AccountKey(123);

                Transaction tx1 = client.transactions().begin();
                Transaction tx2 = client.transactions().begin();

                accounts.put(tx1, key, new Account("John", "Doe", 1000.0d));
                
                Exception err = assertThrows(Exception.class, () -> accounts.put(tx2, key, new Account("John", "Doe", 2000.0d)));

                assertTrue(err.getMessage().contains("Failed to acquire a lock"), err.getMessage());

                tx1.commit();
            }
        } finally {
            System.out.println("\nDropping the table...");

            stmt.executeUpdate("DROP TABLE accounts");

            stmt.close();
            conn.close();
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
