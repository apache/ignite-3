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

package org.apache.ignite.example.table;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.SubmissionPublisher;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.StreamReceiver;
import org.apache.ignite.table.StreamReceiverContext;
import org.apache.ignite.table.Tuple;

public class DataStreamerExample {
    public static void main(String[] args) throws Exception {
        try (var client = IgniteClient.builder().addresses("127.0.0.1:10800").build()) {
            streamDataIntoTable(client);
        }
    }

    private static void streamDataIntoTable(IgniteClient client) {
        var publisher = new SubmissionPublisher<MyData>();

        CompletableFuture<Void> fut = client.tables()
                .table("foo")
                .recordView()
                .streamData(
                        publisher,
                        pojo -> Tuple.create().set("id", pojo.id),
                        new TableUpdateReceiver(),
                        new DataStreamerOptions().batchSize(512));

        publisher.submit(new MyData(1, "abc"));

        fut.join();
    }

    private static void streamDataWithoutTable(IgniteClient client) {
        var publisher = new SubmissionPublisher<MyData>();

        CompletableFuture<Void> fut = client
                .streamData(
                        publisher,
                        new ProcessAndSendToAnotherServiceReceiver(),
                        null);

        publisher.submit(new MyData(2, "xyz"));

        fut.join();
    }

    static class TableUpdateReceiver implements StreamReceiver<MyData> {
        @Override
        public CompletableFuture<Void> receive(Collection<MyData> batch, StreamReceiverContext context) {
            // Transform custom data into tuples and insert.
            for (MyData row : batch) {
                Tuple rec = Tuple.create()
                        .set("id", row.id)
                        .set("name", row.name);

                // Custom logic: use replace to update existing data only.
                // Colocated data can be updated in multiple tables.
                context.table().recordView().replace(null, rec);

                // Update colocated data in another table.
                Tuple colocatedRec = Tuple.create()
                        .set("key", row.id)
                        .set("value", 1);

                context.ignite().tables()
                        .table("bar")
                        .recordView()
                        .upsert(null, colocatedRec);
            }

            return CompletableFuture.completedFuture(null); // Can be a real future in case of async processing.
        }
    }

    static class ProcessAndSendToAnotherServiceReceiver implements StreamReceiver<MyData> {
        @Override
        public CompletableFuture<Void> receive(Collection<MyData> batch, StreamReceiverContext context) {
            // Process data, serialize into JSON and send to some web API.
            return CompletableFuture.completedFuture(null);
        }
    }

    static class MyData {
        public final int id;

        public final String name;

        public MyData(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }
}
