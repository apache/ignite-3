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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.table.Tuple;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.runner.RunnerException;

/**
 * Put then Get benchmark.
 */
public class ClientKvGetBenchmark extends ClientKvBenchmark {
    @Param("1000")
    private int keysPerThread;

    @Param("1000")
    private int loadBatchSize;

    @Override
    public void setUp() {
        super.setUp();

        AtomicInteger locCounter = new AtomicInteger();
        ThreadLocal<Integer> locBase = ThreadLocal.withInitial(() -> offset + locCounter.getAndIncrement() * 20_000_000);
        ThreadLocal<Long> locGen = ThreadLocal.withInitial(() -> 0L);

        Thread[] pool = new Thread[threads];

        System.out.println("Start loading: " + threads);

        for (int i = 0; i < pool.length; i++) {
            pool[i] = new Thread(() -> {
                Map<Tuple, Tuple> batch = new HashMap<>(loadBatchSize);

                for (int i1 = 0; i1 < keysPerThread; i1++) {
                    Tuple key = Tuple.create().set("ycsb_key", nextId(locBase, locGen));
                    batch.put(key, tuple);

                    if (batch.size() == loadBatchSize) {
                        kvView.putAll(null, batch);
                        batch.clear();
                    }
                }
            });
        }

        for (Thread thread : pool) {
            thread.start();
        }

        for (Thread thread : pool) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        System.out.println("Finish loading");
    }

    private final AtomicInteger counter = new AtomicInteger();

    private final ThreadLocal<Integer> base = ThreadLocal.withInitial(() -> offset + counter.getAndIncrement() * 20_000_000);

    private final ThreadLocal<long[]> gen = ThreadLocal.withInitial(() -> new long[1]);

    /**
     * Benchmark for KV upsert via embedded client.
     */
    @Benchmark
    public void get() {
        long[] cur = gen.get();
        cur[0] = cur[0] + 1;

        int id = (int) (base.get() + cur[0] % keysPerThread);

        Tuple key = Tuple.create().set("ycsb_key", id);
        Tuple val = kvView.get(null, key);
        assert val != null : Thread.currentThread().getName() + " " + key;
    }

    /**
     * Benchmark's entry point. Can be started from command line:
     * ./gradlew ":ignite-runner:runClientGetBenchmark" --args='jmh.batch=10 jmh.threads=1'
     */
    public static void main(String[] args) throws RunnerException {
        runBenchmark(ClientKvGetBenchmark.class, args);
    }
}
