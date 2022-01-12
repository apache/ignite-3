package org.apache.ignite.example.tx;

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.tx.IgniteTransactions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.concurrent.CompletableFuture;

/**
 * This example demonstrates the usage of the {@link IgniteTransactions} API.
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
            //--------------------------------------------------------------------------------------
            //
            // Creating an account.
            //
            //--------------------------------------------------------------------------------------

            class AccountKey {
                final int accountNumber;

                public AccountKey(int accountNumber) {
                    this.accountNumber = accountNumber;
                }
            }

            class Account {
                final String firstName;
                final String lastName;
                double balance;

                public Account(String firstName, String lastName, double balance) {
                    this.firstName = firstName;
                    this.lastName = lastName;
                    this.balance = balance;
                }
            }

            KeyValueView<AccountKey, Account> accounts = client.tables()
                .table("PUBLIC.accounts")
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
                accounts.getAsync(tx, key).thenCompose(account -> {
                    account.balance += 300.0d;

                    return accounts.putAsync(tx, key, account);
                })
            );

            // Wait for completion.
            fut.join();

            System.out.println("\nBalance after the async transaction: " + accounts.get(null, key).balance);

            System.out.println("\nDropping the table...");

            try (
                Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800/");
                Statement stmt = conn.createStatement()
            ) {
                stmt.executeUpdate("DROP TABLE accounts");
            }
        }
    }
}
