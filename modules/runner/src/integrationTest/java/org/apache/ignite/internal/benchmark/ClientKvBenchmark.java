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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.internal.client.table.ClientTable;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Tuple;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark for a single upsert operation via KV API with a possibility to disable updates via RAFT and to storage using a remote client.
 */
@State(Scope.Benchmark)
@Fork(1)
@Warmup(iterations = 10, time = 2)
@Measurement(iterations = 20, time = 2)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public abstract class ClientKvBenchmark extends AbstractMultiNodeBenchmark {
    protected final Tuple tuple = Tuple.create();

    protected IgniteClient client;

    protected KeyValueView<Tuple, Tuple> kvView;

    @Param({"0"})
    protected int offset; // 1073741824 for second client to ensure unique keys

    @Param({"32"})
    private int partitionCount;

    @Param({"" + DEFAULT_THREADS_COUNT})
    protected int threads;

    private final AtomicInteger counter = new AtomicInteger();

    private final ThreadLocal<Integer> gen = ThreadLocal.withInitial(() -> offset + counter.getAndIncrement() * 20_000_000);

    protected String[] addresses() {
        String[] nodes = new String[nodes()];

        for (int i = 0; i < nodes.length; i++) {
            nodes[i] = "127.0.0.1:1080" + i;
        }

        return nodes;
    }

    @Override
    public void clusterSetUp() throws Exception {
        if (remote) {
            client = IgniteClient.builder().addresses(addresses()).build();
            publicIgnite = client;
        }
        super.clusterSetUp();
    }

    /**
     * Initializes the tuple.
     */
    @Setup
    public void setUp() {
        for (int i = 1; i < 11; i++) {
            tuple.set("field" + i, FIELD_VAL);
        }

        client = IgniteClient.builder().addresses(addresses()).build();
        ClientTable table = (ClientTable) client.tables().table(TABLE_NAME);
        kvView = table.keyValueView();
    }

    @Override
    public void nodeTearDown() throws Exception {
        closeAll(client);
        super.nodeTearDown();
    }

    protected int nextId() {
        int cur = gen.get() + 1;
        gen.set(cur);
        return cur;
    }

    protected int nextId(ThreadLocal<Integer> base, ThreadLocal<Long> gen) {
        long cur = gen.get();
        gen.set(cur + 1);
        return (int) (base.get() + cur);
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
