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
 * Benchmark                                               Mode  Cnt      Score       Error   Units
 * ClientHeartbeatBenchmark.heartbeat                     thrpt    3  56634.999 ± 14804.164   ops/s
 * ClientHeartbeatBenchmark.heartbeat:gc.alloc.rate       thrpt    3    123.395 ±   100.878  MB/sec
 * ClientHeartbeatBenchmark.heartbeat:gc.alloc.rate.norm  thrpt    3   2298.438 ±  1308.839    B/op
 * ClientHeartbeatBenchmark.heartbeat:gc.count            thrpt    3     33.000              counts
 * ClientHeartbeatBenchmark.heartbeat:gc.time             thrpt    3     25.000                  ms
 * ClientHeartbeatBenchmark.heartbeat:jfr                 thrpt             NaN                 ---
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
                .measurementIterations(3)
                .measurementTime(TimeValue.seconds(15))
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}
