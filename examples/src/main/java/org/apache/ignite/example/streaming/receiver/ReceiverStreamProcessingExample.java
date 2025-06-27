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

package org.apache.ignite.example.streaming.receiver;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.example.streaming.pojo.Account;
import org.apache.ignite.example.streaming.pojo.Trade;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.DataStreamerReceiver;
import org.apache.ignite.table.DataStreamerReceiverContext;
import org.apache.ignite.table.DataStreamerTarget;
import org.apache.ignite.table.ReceiverDescriptor;
import org.apache.ignite.table.RecordView;

/**
 * This example demonstrates the usage of the
 * {@link DataStreamerTarget#streamData(Publisher, Function, Function, ReceiverDescriptor, Subscriber, DataStreamerOptions, Object)} API
 * for stream processing of the trades data read from the file.
 *
 * <p>Find instructions on how to run the example in the README.md file located in the "examples" directory root.
 *
 * <p>The following steps related to code deployment should be additionally executed before running the current example:
 * <ol>
 *     <li>
 *         Build "ignite-examples-x.y.z.jar" using the next command:<br>
 *         {@code ./gradlew :ignite-examples:jar}
 *     </li>
 *     <li>
 *         Create a new deployment unit using the CLI tool:<br>
 *         {@code cluster unit deploy receiverExampleUnit \
 *          --version 1.0.0 \
 *          --path=$IGNITE_HOME/examples/build/libs/ignite-examples-x.y.z.jar}
 *     </li>
 * </ol>
 */
public class ReceiverStreamProcessingExample {
    /** Number of trades to load. */
    private static final int TRADES_COUNT = 10_000;

    /** Number of accounts to load. */
    private static final int ACCOUNTS_COUNT = 100;

    /** Deployment unit name. */
    private static final String DEPLOYMENT_UNIT_NAME = "receiverExampleUnit";

    /** Deployment unit version. */
    private static final String DEPLOYMENT_UNIT_VERSION = "1.0.0";

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

        System.out.println("\nCreating 'accounts' table...");

        try (
                Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1:10800/");
                Statement stmt = conn.createStatement()
        ) {
            stmt.executeUpdate(
                    "CREATE TABLE accounts ("
                            + "accountNumber INT PRIMARY KEY,"
                            + "name          VARCHAR,"
                            + "balance       BIGINT,"
                            + "active        BOOLEAN)"
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
            // Creating a record accountView for the 'accounts' table.
            //
            //--------------------------------------------------------------------------------------

            RecordView<Account> accountView = client.tables().table("accounts").recordView(Account.class);

            //--------------------------------------------------------------------------------------
            //
            // Creating account records.
            //
            //--------------------------------------------------------------------------------------

            System.out.println("\nCreating account records...");

            for (int i = 0; i < ACCOUNTS_COUNT; i++) {
                accountView.insert(null, new Account(i, "name" + i, 0L, ThreadLocalRandom.current().nextBoolean()));
            }

            //--------------------------------------------------------------------------------------
            //
            // Creating publisher.
            //
            //--------------------------------------------------------------------------------------

            System.out.println("\nCreating publisher...");

            long start = System.currentTimeMillis();

            CompletableFuture<Void> streamerFut;

            try (var publisher = new SubmissionPublisher<String>()) {
                //--------------------------------------------------------------------------------------
                //
                // Configuring data streamer.
                //
                //--------------------------------------------------------------------------------------

                System.out.println("\nConfiguring data streamer...");

                ReceiverDescriptor<Object> receiver = ReceiverDescriptor.builder(TradeProcessingReceiver.class)
                        .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
                        .build();

                Function<String, Account> keyFunc = trade -> new Account(Integer.parseInt(trade.substring(5, 9)));

                streamerFut = accountView.streamData(publisher, keyFunc, t -> t, receiver, null, null, null);

                //--------------------------------------------------------------------------------------
                //
                // Performing the 'streaming' operation.
                //
                //--------------------------------------------------------------------------------------

                System.out.println("\nStreaming trades data...");

                streamTradesDataFromFile(publisher);
            }

            //--------------------------------------------------------------------------------------
            //
            // Waiting for data to be flushed.
            //
            //--------------------------------------------------------------------------------------

            streamerFut.join();

            long end = System.currentTimeMillis();

            System.out.println("\nStreamed " + TRADES_COUNT + " trades in " + (end - start) + "ms.");
        } finally {
            //--------------------------------------------------------------------------------------
            //
            // Dropping the table.
            //
            //--------------------------------------------------------------------------------------

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
     * Reads trades data from csv file and streams it using provided publisher.
     *
     * @param publisher Publisher to be used to stream trades.
     * @throws IOException In case of error.
     */
    private static void streamTradesDataFromFile(SubmissionPublisher<String> publisher) throws IOException {
        assert publisher != null;

        InputStream stream = ReceiverStreamProcessingExample.class.getResourceAsStream("/tradesData.csv");

        assert stream != null;

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(stream))) {
            reader.lines().forEach(publisher::submit);
        }
    }

    /**
     * The receiver parses and processes raw trade data.
     */
    private static class TradeProcessingReceiver implements DataStreamerReceiver<String, Object, Object> {
        /** {@inheritDoc} */
        @Override
        public CompletableFuture<List<Object>> receive(List<String> page, DataStreamerReceiverContext ctx, Object arg) {
            RecordView<Account> accountsView = ctx.ignite().tables().table("accounts").recordView(Account.class);

            page.forEach(rawTradeData -> processRawTradeData(rawTradeData, accountsView));

            return CompletableFuture.completedFuture(null);
        }

        /**
         * Processes provided raw trade data.
         *
         * @param rawTradeData Raw trade data to process.
         * @param accountsView Accounts view.
         */
        private static void processRawTradeData(String rawTradeData, RecordView<Account> accountsView) {
            Trade trade = parseRawTradeData(rawTradeData);

            Account account = accountsView.get(null, new Account(trade.getAccountNumber()));

            if (!account.isActive())
                return;

            System.out.println("The trade with id=" + trade.getTradeId() + " for account with accountNumber=" +
                    account.getAccountNumber() + " was processed.");
        }

        /**
         * Parses raw trade data into {@link Trade} object.
         *
         * @param rawTradeData Raw trade data to parse.
         * @return Parsed {@link Trade} object.
         */
        private static Trade parseRawTradeData(String rawTradeData) {
            String[] split = rawTradeData.split(",");

            assert split.length == 3;

            int tradeId = Integer.parseInt(split[0]);
            int accountNumber = Integer.parseInt(split[1]);
            int amount = Integer.parseInt(split[2]);

            return new Trade(tradeId, accountNumber, amount);
        }
    }
}
