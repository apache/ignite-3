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

package org.apache.ignite.client.benchmarks;

import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.TestServer;
import org.apache.ignite.client.fakes.FakeIgnite;
import org.apache.ignite.internal.client.ClientChannel;
import org.apache.ignite.internal.client.TcpIgniteClient;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

/**
 * Heartbeat (ping) benchmark - measures the simplest client-server interaction.
 *
 * <p>Results on i9-12900H, openjdk 11.0.23, Ubuntu 22.04:
 * Benchmark                                                          Mode  Cnt      Score       Error   Units
 * ClientHeartbeatBenchmark.heartbeat                                thrpt    5  16274.873 ±  1306.424   ops/s
 * ClientHeartbeatBenchmark.heartbeat:gc.alloc.rate                  thrpt    5    750.803 ±   851.850  MB/sec
 * ClientHeartbeatBenchmark.heartbeat:gc.alloc.rate.norm             thrpt    5  48292.682 ± 53619.130    B/op
 * ClientHeartbeatBenchmark.heartbeat:gc.count                       thrpt    5    122.000              counts
 * ClientHeartbeatBenchmark.heartbeat:gc.time                        thrpt    5     60.000                  ms
 * ClientHeartbeatBenchmark.heartbeatWithPayload                     thrpt    5  15878.426 ±   511.951   ops/s
 * ClientHeartbeatBenchmark.heartbeatWithPayload:gc.alloc.rate       thrpt    5    742.915 ±   821.941  MB/sec
 * ClientHeartbeatBenchmark.heartbeatWithPayload:gc.alloc.rate.norm  thrpt    5  49020.734 ± 53618.819    B/op
 * ClientHeartbeatBenchmark.heartbeatWithPayload:gc.count            thrpt    5    120.000              counts
 * ClientHeartbeatBenchmark.heartbeatWithPayload:gc.time             thrpt    5     62.000                  ms
 */
@State(Scope.Benchmark)
public class ClientHeartbeatBenchmark {
    private static final byte[] payload = new byte[1024];

    private TestServer testServer;

    private TcpIgniteClient client;

    private ClientChannel channel;

    /**
     * Init.
     */
    @Setup
    public void init() {
        ThreadLocalRandom.current().nextBytes(payload);

        testServer = new TestServer(1000, new FakeIgnite("server-1"));

        client = (TcpIgniteClient) IgniteClient.builder()
                .addresses("127.0.0.1:" + testServer.port())
                .build();

        channel = client.channel().channels().get(0);
    }

    /**
     * Tear down.
     */
    @TearDown
    public void tearDown() throws Exception {
        client.close();
        testServer.close();
    }

    /**
     * Heartbeat benchmark.
     */
    @Benchmark
    public void heartbeat() {
        channel.heartbeatAsync(null).join();
    }

    /**
     * Runner.
     *
     * @param args Arguments.
     * @throws RunnerException Exception.
     */
    public static void main(String[] args) throws RunnerException {
        ResourceLeakDetector.setLevel(Level.DISABLED);

        Options opt = new OptionsBuilder()
                .include(ClientHeartbeatBenchmark.class.getSimpleName())
                .mode(Mode.Throughput)
                .addProfiler("gc")
                .addProfiler("jfr")
                .warmupIterations(3)
                .warmupTime(TimeValue.seconds(3))
                .measurementIterations(5)
                .measurementTime(TimeValue.seconds(5))
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}
