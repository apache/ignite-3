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
import java.util.stream.Collectors;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.example.streaming.pojo.Account;
import org.apache.ignite.example.streaming.pojo.Trade;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.DataStreamerReceiver;
import org.apache.ignite.table.DataStreamerReceiverContext;
import org.apache.ignite.table.DataStreamerTarget;
import org.apache.ignite.table.ReceiverDescriptor;
import org.apache.ignite.table.RecordView;

/**
 * This example demonstrates the usage of the
 * {@link DataStreamerTarget#streamData(Publisher, Function, Function, ReceiverDescriptor, Subscriber, DataStreamerOptions, Object)} API
 * for stream processing of the trade data and updating account data in the table.
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
public class ReceiverStreamProcessingWithTableUpdateExample {
    /** Number of trades to load. */
    private static final int TRADES_COUNT = 10_000;

    /** Number of accounts to load. */
    private static final int ACCOUNTS_COUNT = 100;

    /** Trade multiplier. */
    private static final int TRADE_MULTIPLIER = 2;

    /** Trade minimal amount. */
    private static final int TRADE_MIN_AMOUNT = 10;

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
            // Creating a record view for the 'accounts' table.
            //
            //--------------------------------------------------------------------------------------

            RecordView<Account> view = client.tables().table("accounts").recordView(Account.class);

            //--------------------------------------------------------------------------------------
            //
            // Creating account records.
            //
            //--------------------------------------------------------------------------------------

            System.out.println("\nCreating account records...");

            for (int i = 0; i < ACCOUNTS_COUNT; i++) {
                view.insert(null, new Account(i, "name" + i, 0L, ThreadLocalRandom.current().nextBoolean()));
            }

            //--------------------------------------------------------------------------------------
            //
            // Creating publisher.
            //
            //--------------------------------------------------------------------------------------

            System.out.println("\nCreating publisher...");

            long start = System.currentTimeMillis();

            CompletableFuture<Void> streamerFut;

            try (var publisher = new SubmissionPublisher<Trade>()) {
                //--------------------------------------------------------------------------------------
                //
                // Configuring data streamer.
                //
                //--------------------------------------------------------------------------------------

                System.out.println("\nConfiguring data streamer...");

                Function<Trade, Account> keyFunc = trade -> new Account(trade.getAccountNumber());

                Function<Trade, byte[]> payloadFunc = Trade::toByteArray;

                ReceiverDescriptor<String> receiver = ReceiverDescriptor.builder(TradeProcessingReceiver.class)
                        .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
                        .build();

                DataStreamerOptions options = DataStreamerOptions.builder()
                        .autoFlushInterval(1000)
                        .build();

                String receiverArgs = TRADE_MIN_AMOUNT + ":" + TRADE_MULTIPLIER;

                streamerFut = view.streamData(
                        publisher,
                        keyFunc,
                        payloadFunc,
                        receiver,
                        null,
                        options,
                        receiverArgs
                );

                //--------------------------------------------------------------------------------------
                //
                // Performing the 'streaming' operation.
                //
                //--------------------------------------------------------------------------------------

                System.out.println("\nStreaming trade data...");

                streamTradesData(publisher);
            }

            //--------------------------------------------------------------------------------------
            //
            // Waiting for data to be flushed.
            //
            //--------------------------------------------------------------------------------------

            streamerFut.join();

            long end = System.currentTimeMillis();

            System.out.println("\nStreamed " + TRADES_COUNT + " trades in " + (end - start) + "ms.");

            //--------------------------------------------------------------------------------------
            //
            // Checking the entries count.
            //
            //--------------------------------------------------------------------------------------

            client.transactions().runInTransaction(tx -> {
                try (ResultSet<SqlRow> rs = client.sql().execute(tx, "SELECT accountNumber, balance FROM accounts")) {
                    System.out.println("\nCurrent account balances:");

                    while (rs.hasNext()) {
                        SqlRow row = rs.next();

                        System.out.println("accountNumber=" + row.intValue(0) + ", balance=" + row.longValue(1));
                    }
                }
            });
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
     * Generates {@link Trade} objects and streams them using provided publisher.
     *
     * @param publisher Publisher to be used to stream trades.
     */
    private static void streamTradesData(SubmissionPublisher<Trade> publisher) {
        assert publisher != null;

        for (int i = 0; i < TRADES_COUNT; i++) {
            Trade trade = new Trade(i, i % ACCOUNTS_COUNT, ThreadLocalRandom.current().nextInt(100));

            publisher.submit(trade);

            if (i > 0 && i % 1000 == 0)
                System.out.println("Streamed " + i + " trades.");
        }
    }

    /**
     * The receiver processes trade data based on the provided arg and updates balance for the account.
     */
    private static class TradeProcessingReceiver implements DataStreamerReceiver<byte[], String, Object> {
        /** {@inheritDoc} */
        @Override
        public CompletableFuture<List<Object>> receive(List<byte[]> page, DataStreamerReceiverContext ctx, String arg) {
            //--------------------------------------------------------------------------------------
            //
            // Parsing receiver arguments.
            //
            //--------------------------------------------------------------------------------------

            System.out.println("\nParsing receiver arguments...");

            TradeReceiverArgs parsedArgs = TradeReceiverArgs.from(arg);

            //--------------------------------------------------------------------------------------
            //
            // Parsing trades.
            //
            //--------------------------------------------------------------------------------------

            System.out.println("\nParsing trades...");

            List<Trade> trades = page.stream().map(Trade::parseTrade).collect(Collectors.toList());

            //--------------------------------------------------------------------------------------
            //
            // Processing trades.
            //
            //--------------------------------------------------------------------------------------

            System.out.println("\nProcessing trades...");

            long start = System.currentTimeMillis();

            processTrades(trades, ctx, parsedArgs);

            System.out.println("\n" + page.size() + " trades processed in " + (System.currentTimeMillis() - start) + "ms.");

            return CompletableFuture.completedFuture(null);
        }

        /**
         * Processes trades.
         *
         * @param trades Trades to process.
         * @param ctx Receiver context.
         * @param args Arguments.
         */
        private static void processTrades(List<Trade> trades, DataStreamerReceiverContext ctx, TradeReceiverArgs args) {
            assert trades != null;
            assert ctx != null;
            assert args != null;

            RecordView<Account> view = ctx.ignite().tables().table("accounts").recordView(Account.class);

            for (Trade trade : trades) {
                int adjustedAmount = trade.getAmount() * args.multiplier;

                if (adjustedAmount < args.minimalAmount)
                    continue;

                ctx.ignite().transactions().runInTransaction(tx -> {
                    Account account = view.get(tx, new Account(trade.getAccountNumber()));

                    account.setBalance(account.getBalance() + adjustedAmount);

                    view.upsert(tx, account);
                });
            }
        }
    }

    /**
     * Trade receiver arguments.
     */
    private static class TradeReceiverArgs {
        /** Minimal trade amount. */
        private final int minimalAmount;

        /** Trade amount multiplier. */
        private final int multiplier;

        /**
         * Constructor.
         *
         * @param minimalAmount Minimal trade amount.
         * @param multiplier Trade amount multiplier.
         */
        TradeReceiverArgs(int minimalAmount, int multiplier) {
            this.minimalAmount = minimalAmount;
            this.multiplier = multiplier;
        }

        /**
         * Deserializes {@link TradeReceiverArgs} object from the provided string.
         *
         * @param str Serialized {@link TradeReceiverArgs} object.
         * @return Deserialized {@link TradeReceiverArgs} object.
         */
        static TradeReceiverArgs from(String str) {
            if (str == null) {
                return new TradeReceiverArgs(0, 1);
            }

            String[] split = str.split(":");
            int minimalAmount = split.length == 2 ? Integer.parseInt(split[0]) : 0;
            int multiplier = split.length == 2 ? Integer.parseInt(split[1]) : 1;

            return new TradeReceiverArgs(minimalAmount, multiplier);
        }
    }
}
