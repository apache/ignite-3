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
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
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
 * for stream processing of the trade data and receiving processing results.
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
public class ReceiverStreamProcessingWithResultSubscriberExample {
    /** Number of trades to load. */
    private static final int TRADES_COUNT = 10_000;

    /** Number of accounts to load. */
    private static final int ACCOUNTS_COUNT = 100;

    /** The warning threshold used in {@link TradeProcessingReceiver}. */
    private static final int TRADE_WARNING_THRESHOLD = 90;

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

            CompletableFuture<Void> subscriberFut = new CompletableFuture<>();

            try (var publisher = new SubmissionPublisher<Trade>()) {
                //--------------------------------------------------------------------------------------
                //
                // Configuring data streamer.
                //
                //--------------------------------------------------------------------------------------

                System.out.println("\nConfiguring data streamer...");

                Function<Trade, Account> keyFunc = trade -> new Account(trade.getAccountNumber());

                Function<Trade, byte[]> payloadFunc = Trade::toByteArray;

                ReceiverDescriptor<Object> receiver = ReceiverDescriptor.builder(TradeProcessingReceiver.class)
                        .units(new DeploymentUnit(DEPLOYMENT_UNIT_NAME, DEPLOYMENT_UNIT_VERSION))
                        .build();

                TradeProcessingResultSubscriber resultSubscriber = new TradeProcessingResultSubscriber(subscriberFut);

                DataStreamerOptions options = DataStreamerOptions.builder()
                        .autoFlushInterval(1000)
                        .build();

                streamerFut = view.streamData(
                        publisher,
                        keyFunc,
                        payloadFunc,
                        receiver,
                        resultSubscriber,
                        options,
                        null
                );

                //--------------------------------------------------------------------------------------
                //
                // Performing the 'streaming' operation.
                //
                //--------------------------------------------------------------------------------------

                System.out.println("\nStreaming trades data...");

                streamTradesData(publisher);
            }

            //--------------------------------------------------------------------------------------
            //
            // Waiting for data to be flushed and the results to be processed by the subscriber.
            //
            //--------------------------------------------------------------------------------------

            streamerFut.join();

            subscriberFut.join();

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
     * The receiver processes trade data and returns processing results in the String format.
     */
    private static class TradeProcessingReceiver implements DataStreamerReceiver<byte[], Object, String> {
        /** {@inheritDoc} */
        @Override
        public CompletableFuture<List<String>> receive(List<byte[]> page, DataStreamerReceiverContext ctx, Object arg) {
            RecordView<Account> view = ctx.ignite().tables().table("accounts").recordView(Account.class);

            return CompletableFuture.completedFuture(page.stream()
                    .map(Trade::parseTrade)
                    .map(trade -> {
                        Account account = view.get(null, new Account(trade.getAccountNumber()));

                        return processTrade(trade, account);
                    })
                    .collect(Collectors.toList()));
        }

        /**
         * Processes received {@link Trade} object.
         *
         * @param trade Trade to be processed.
         * @param account Account associated with the trade.
         * @return Trade processing result.
         */
        private static String processTrade(Trade trade, Account account) {
            if (account.isActive())
                return trade.getTradeId() + ":" + Boolean.FALSE;

            if (trade.getAmount() > TRADE_WARNING_THRESHOLD)
                System.out.println("The trade has reached the threshold: Trade [id=" + trade.getTradeId() + ", accountNumber=" +
                        trade.getAccountNumber() + ", amount=" + trade.getAmount() + "]." );

            return trade.getTradeId() + ":" + Boolean.TRUE;
        }
    }

    /**
     * Subscriber for the trade processing results.
     */
    private static class TradeProcessingResultSubscriber implements Subscriber<String> {
        /** Subscriber future. */
        private final CompletableFuture<Void> subscriberFut;

        /** Subscription. */
        private Subscription subscription;

        /**
         * Constructor.
         *
         * @param subscriberFut Subscriber future.
         */
        TradeProcessingResultSubscriber(CompletableFuture<Void> subscriberFut) {
            this.subscriberFut = subscriberFut;
        }

        /** {@inheritDoc} */
        @Override
        public void onSubscribe(Subscription subscription) {
            this.subscription = subscription;
        }

        /** {@inheritDoc} */
        @Override
        public void onNext(String item) {
            assert item != null;

            String[] split = item.split(":");

            assert split.length == 2;

            int tradeId = Integer.parseInt(split[0]);

            boolean processed = Boolean.parseBoolean(split[1]);

            System.out.println("Received trade processing result [tradeId=" + tradeId + ", processed=" + processed + "]");
        }

        /** {@inheritDoc} */
        @Override
        public void onError(Throwable throwable) {
            System.out.println("An error occurred during processing: " + throwable.getMessage());

            subscriberFut.completeExceptionally(throwable);
        }

        /** {@inheritDoc} */
        @Override
        public void onComplete() {
            System.out.println("Processing completed.");

            subscriberFut.complete(null);
        }
    }
}
