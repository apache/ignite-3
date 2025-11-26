package org.apache.ignite.example.streaming;
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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.DataStreamerItem;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.RecordView;

/**
 * This example demonstrates how to use the streaming API to configure the data streamer, insert account records into the existing Accounts
 * table and then delete them.
 */

public class SingleTableDataStreamerExample {

    private static final int ACCOUNTS_COUNT = 10;

    /* Assuming table Accounts exists */
    public static void main(String[] arg) {

        try (IgniteClient client = IgniteClient.builder()
                .addresses("127.0.0.1:10800")
                .build()) {
            RecordView<Account> view = client.tables().table("Accounts").recordView(Account.class);

            /* PUT entries into the table*/
            streamAccountDataPut(view);

            /* Verify that table contains these entries by reading them back */
            verifyPut(view);

            /* Remove entries from the table */
            streamAccountDataRemove(view);

            /* Check that table doesnt contain data */
            verifyRemove(view);

        }
    }


    /* Streaming data using DataStreamerOperationType#PUT operation */
    private static void streamAccountDataPut(RecordView<Account> view) {
        DataStreamerOptions options = DataStreamerOptions.builder()
                .pageSize(1000)
                .perPartitionParallelOperations(1)
                .autoFlushInterval(1000)
                .retryLimit(16)
                .build();

        CompletableFuture<Void> streamerFut;
        try (var publisher = new SubmissionPublisher<DataStreamerItem<Account>>()) {

            streamerFut = view.streamData(publisher, options);

            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            for (int i = 0; i < ACCOUNTS_COUNT; i++) {
                Account entry = new Account(i, "name" + i, rnd.nextLong(100_000), rnd.nextBoolean());
                publisher.submit(DataStreamerItem.of(entry));
            }
        }
        streamerFut.join();
    }


    /* Streaming data using DataStreamerOperationType#REMOVE operation */
    private static void streamAccountDataRemove(RecordView<Account> view) {
        DataStreamerOptions options = DataStreamerOptions.builder()
                .pageSize(1000)
                .perPartitionParallelOperations(1)
                .autoFlushInterval(1000)
                .retryLimit(16)
                .build();

        CompletableFuture<Void> streamerFut;
        try (var publisher = new SubmissionPublisher<DataStreamerItem<Account>>()) {
            streamerFut = view.streamData(publisher, options);
            for (int i = 0; i < ACCOUNTS_COUNT; i++) {
                Account entry = new Account(i);
                publisher.submit(DataStreamerItem.removed(entry));
            }
        }
        streamerFut.join();
    }

    private static void verifyPut(RecordView view) {
        System.out.println("=== Table data after PUT ===");
        for (int i = 0; i < ACCOUNTS_COUNT; i++) {
            Account keyRec = new Account(i);
            if (view.contains(null, keyRec)) {
                Account record = (Account) view.get(null, keyRec);
                System.out.printf("Found: id=%d, name=%s, balance=%d, active=%b%n",
                        record.getId(), record.getName(), record.getBalance(), record.isActive());
            } else {
                System.out.printf("Missing id=%d%n", i);
            }
        }
    }

    private static void verifyRemove(RecordView<Account> view) {
        System.out.println("=== Table data after REMOVE ===");
        List<Account> keys = IntStream.range(0, ACCOUNTS_COUNT)
                .mapToObj(Account::new)
                .collect(Collectors.toList());
        List<Account> records = view.getAll(null, keys);
        for (int i = 0; i < records.size(); i++) {
            System.out.printf("id=%d exists? %b%n", i, records.get(i) != null);
        }
    }
}
