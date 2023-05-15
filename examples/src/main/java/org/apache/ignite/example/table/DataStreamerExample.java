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
                        null,
                        new DataStreamerOptions().batchSize(512));

        publisher.submit(new MyData(1, "abc"));

        fut.join();
    }

    static class TableUpdateReceiver implements StreamReceiver<MyData, MyResult> {
        @Override
        public CompletableFuture<MyResult> receive(MyData row, StreamReceiverContext context) {
            try {

                // Transform custom data into tuples and insert.
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

                // Return null to indicate success and avoid sending result back to the client.
                // Can be a real future in case of async processing.
                return CompletableFuture.completedFuture(null);
            } catch (Exception e) {
                // Return problematic result to the client.
                return CompletableFuture.completedFuture(new MyResult(row, "Could not process item: " + e.getMessage()));
            }
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

    static class MyResult {
        public final MyData item;

        public final String problem;

        public MyResult(MyData item, String problem) {
            this.item = item;
            this.problem = problem;
        }
    }
}
