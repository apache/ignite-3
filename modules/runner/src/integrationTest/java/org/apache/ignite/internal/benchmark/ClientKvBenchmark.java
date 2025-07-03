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

import static org.apache.ignite.internal.util.IgniteUtils.closeAll;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.client.table.ClientTable;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark for a single upsert operation via KV API with a possibility to disable updates via RAFT and to storage using a remote client.
 */
@State(Scope.Benchmark)
@Fork(1)
@Threads(32)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 20, time = 2)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class ClientKvBenchmark extends AbstractMultiNodeBenchmark {
    protected IgniteClient client;

    private KeyValueView<Tuple, Tuple> kvView;

    @Param({"0"})
    private int offset; // 1073741824 for second client to ensure unique keys

    @Param({"1", "10"})
    private int batch;

    @Param({"false"})
    private boolean fsync;

    @Param({"32"})
    private int partitionCount;

    @Param({"100"})
    private int fieldLength;

    @Param({"uniquePrefix"/*, "uniquePostfix"*/})
    private String fieldValueGeneration;

    @Param({/*"false",*/ "true"})
    private boolean withTx;

    @Param({"false", "true"})
    private boolean updateWiList;

    @Param({"false", "true"})
    private boolean igniteSkipWriteIntentSwitch;

    private final AtomicInteger counter = new AtomicInteger();

    private final ThreadLocal<Integer> gen = ThreadLocal.withInitial(() -> offset + counter.getAndIncrement() * 20_000_000);

    protected String[] addresses() {
        return new String[]{"127.0.0.1:10800"/*, "127.0.0.1:10801"*/};
    }

    @Override
    public void nodeSetUp() throws Exception {
//        System.setProperty(IgniteSystemProperties.IGNITE_SKIP_REPLICATION_IN_BENCHMARK, "false");
//        System.setProperty(IgniteSystemProperties.IGNITE_SKIP_STORAGE_UPDATE_IN_BENCHMARK, "false");
        System.setProperty("IGNITE_UPDATE_WI_LIST", Boolean.toString(updateWiList));
        System.setProperty("IGNITE_SKIP_WRITE_INTENT_SWITCH", Boolean.toString(igniteSkipWriteIntentSwitch));
        super.nodeSetUp();
    }

    /**
     * Initializes the tuple.
     */
    @Setup
    public void setUp() {
        client = IgniteClient.builder().addresses(addresses()).build();
        ClientTable table = (ClientTable) client.tables().table(TABLE_NAME);
        kvView = table.keyValueView();
    }

    private Tuple valueTuple(int id) {
        String formattedString = String.format("%" + (fieldValueGeneration.equals("uniquePrefix") ? '-' : '0') + fieldLength + "d", id);

        String fieldVal = formattedString.length() > fieldLength ? formattedString.substring(0, fieldLength) : formattedString;

        return Tuple.create()
                .set("field1", fieldVal)
                .set("field2", fieldVal)
                .set("field3", fieldVal)
                .set("field4", fieldVal)
                .set("field5", fieldVal)
                .set("field6", fieldVal)
                .set("field7", fieldVal)
                .set("field8", fieldVal)
                .set("field9", fieldVal)
                .set("field10", fieldVal);
    }

    @Override
    public void nodeTearDown() throws Exception {
        closeAll(client);
        super.nodeTearDown();
    }

    /**
     * Benchmark for KV upsert via embedded client.
     */
    @Benchmark
    public void upsert() {
        Transaction tx = withTx ? client.transactions().begin() : null;

        List<CompletableFuture<Void>> futs = new ArrayList<>();

        for (int i = 0; i < batch - 1; i++) {
            int id = nextId();

            CompletableFuture<Void> fut = kvView.putAsync(tx, Tuple.create().set("ycsb_key", id), valueTuple(id));
            futs.add(fut);
        }

        CompletableFutures.allOf(futs).join();

        int id = nextId();

        kvView.put(tx, Tuple.create().set("ycsb_key", id), valueTuple(id));

        if (withTx) {
            tx.commit();
        }
    }

    private int nextId() {
        int cur = gen.get() + 1;
        gen.set(cur);
        return cur;
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + ClientKvBenchmark.class.getSimpleName() + ".*")
                // .jvmArgsAppend("-Djmh.executor=VIRTUAL")
                // .addProfiler(JavaFlightRecorderProfiler.class, "configName=profile.jfc")
                .build();

        new Runner(opt).run();
    }

    @Override
    protected boolean fsync() {
        return fsync;
    }

    @Override
    protected int nodes() {
        return 1;
    }

    @Override
    protected int partitionCount() {
        return partitionCount;
    }

    @Override
    protected int replicaCount() {
        return 1;
    }
}
