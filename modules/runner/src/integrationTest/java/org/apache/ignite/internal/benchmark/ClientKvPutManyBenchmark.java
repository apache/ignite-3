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

package org.apache.ignite.internal.benchmark;

import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.runner.RunnerException;

/**
 * Put many benchmark.
 */
public class ClientKvPutManyBenchmark extends ClientKvBenchmark {
    @Param({"5"})
    private int batch;

    /**
     * Benchmark for KV upsert via embedded client.
     */
    @Benchmark
    public void upsert() {
        Transaction tx = client.transactions().begin();
        for (int i = 0; i < batch; i++) {
            Tuple key = Tuple.create().set("ycsb_key", nextId());
            kvView.put(tx, key, tuple);
        }
        tx.commit();
    }

    /**
     * Benchmark's entry point. Can be started from command line:
     * ./gradlew ":ignite-runner:runClientPutBenchmark" --args='jmh.batch=10 jmh.threads=1'
     */
    public static void main(String[] args) throws RunnerException {
        runBenchmark(ClientKvPutManyBenchmark.class, args);
    }
}
