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

package org.apache.ignite.example.streaming;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.DataStreamerException;
import org.apache.ignite.table.DataStreamerItem;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.RecordView;

/**
 * This example demonstrates how to use the streaming API to catch both asynchronous errors during background streaming and immediate submission errors.
 * If data streamer fails to process any entries, it collects the failed items in a DataStreamerException.
 */

public class DetectFailedEntriesExample {

    private static final int ACCOUNTS_COUNT = 10;

    public static void main(String[] arg) {

        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()) {

            /* Configure streaming options */
            DataStreamerOptions options = DataStreamerOptions.builder()
                    .pageSize(1000)
                    .perPartitionParallelOperations(1)
                    .autoFlushInterval(1000)
                    .retryLimit(16)
                    .build();
            System.out.println("Creating Accounts table");
            client.sql().execute("CREATE TABLE IF NOT EXISTS ACCOUNTS (id INT PRIMARY KEY, name VARCHAR(255), balance BIGINT, active BOOLEAN);");

            RecordView<Account> view = client.tables()
                    .table("Accounts")
                    .recordView(Account.class);

            SubmissionPublisher<DataStreamerItem<Account>> publisher = new SubmissionPublisher<>();
            CompletableFuture<Void> streamerFut = view.streamData(publisher, options);
            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            /* Wrap our stream in a publisher and attach failure handlers */
            try (publisher) {

                view.streamData(publisher, options)
                        .exceptionally(e -> {

                            /* Asynchronous failures during background streaming */
                            Throwable cause = e.getCause();
                            if (cause instanceof DataStreamerException) {
                                System.out.println("Failed items during background streaming: " +
                                        ((DataStreamerException) cause).failedItems());
                            } else {
                                System.out.println("Streaming error: " + cause.getMessage());
                            }
                            return null;
                        });

                /* Publish entries */
                for (int i = 0; i < ACCOUNTS_COUNT; i++) {
                    Account entry = new Account(
                            i,
                            "Account " + i,
                            rnd.nextLong(100_000),
                            rnd.nextBoolean()
                    );
                    publisher.submit(DataStreamerItem.of(entry));
                }
            } catch (DataStreamerException e) {
                /* Immediate failures during submission */
                System.out.println("Failed items during submission: " + e.failedItems());
            }

            /* Wait for background streaming to complete */
            streamerFut.join();

            System.out.println("Dropping Accounts table.");
            client.sql().execute("DROP TABLE IF EXISTS ACCOUNTS;");
        }
    }
}