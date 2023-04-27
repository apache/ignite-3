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
import org.apache.ignite.table.Tuple;

public class DataStreamerExample {
    public static void main(String[] args) throws Exception {
        try (var client = IgniteClient.builder().addresses("127.0.0.1:10800").build()) {
            var publisher = new SubmissionPublisher<Tuple>();

            CompletableFuture<Void> fut = client.tables().table("foo").recordView().streamData(publisher, null);

            publisher.submit(Tuple.create().set("key", 1).set("value", "value1"));

            fut.join();
        }
    }
}
