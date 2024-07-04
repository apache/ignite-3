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

package org.apache.ignite.internal.benchmarks;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.network.MessageSerializationRegistryImpl;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesSerializationRegistryInitializer;
import org.apache.ignite.internal.partition.replicator.network.command.TimedBinaryRowMessage;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateAllCommand;
import org.apache.ignite.internal.partition.replicator.network.command.UpdateCommand;
import org.apache.ignite.internal.raft.Marshaller;
import org.apache.ignite.internal.raft.util.OptimizedMarshaller;
import org.apache.ignite.internal.raft.util.ThreadLocalOptimizedMarshaller;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * A micro-benchmark of {@link OptimizedMarshaller}.
 */
@Warmup(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class UpdateCommandsMarshalingMicroBenchmark {
    private static final PartitionReplicationMessagesFactory PARTITION_REPLICATION_MESSAGES_FACTORY =
            new PartitionReplicationMessagesFactory();

    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    private static final MessageSerializationRegistry REGISTRY = new MessageSerializationRegistryImpl();

    private static final Marshaller MARSHALLER = new ThreadLocalOptimizedMarshaller(REGISTRY);

    /** Binary tuple size in bytes. */
    @Param({"128", "2048", "8192"})
    private int payloadSize;

    /** Whether we create {@link UpdateCommand} or {@link UpdateAllCommand}. */
    @Param({"false", "true"})
    private boolean updateAll;

    private NetworkMessage message;

    static {
        new PartitionReplicationMessagesSerializationRegistryInitializer().registerFactories(REGISTRY);
    }

    private byte[] messageBytes;

    /**
     * Initializes {@link #message} and {@link #messageBytes}.
     */
    @Setup(Level.Trial)
    public void setUp() {
        byte[] array = new byte[payloadSize];

        UUID uuid = UUID.randomUUID();
        long timestamp = System.currentTimeMillis();

        TimedBinaryRowMessage timedBinaryRowMessage = PARTITION_REPLICATION_MESSAGES_FACTORY.timedBinaryRowMessage()
                .timestamp(timestamp)
                .binaryRowMessage(PARTITION_REPLICATION_MESSAGES_FACTORY.binaryRowMessage()
                        .schemaVersion(128)
                        .binaryTuple(ByteBuffer.wrap(array))
                        .build())
                .build();

        if (updateAll) {
            Map<UUID, TimedBinaryRowMessage> map = new HashMap<>();
            for (int i = 0; i < 100; i++) {
                map.put(UUID.randomUUID(), timedBinaryRowMessage);
            }
            message = PARTITION_REPLICATION_MESSAGES_FACTORY.updateAllCommand()
                    .txId(uuid)
                    .leaseStartTime(timestamp)
                    .safeTimeLong(timestamp)
                    .requiredCatalogVersion(10_000)
                    .tablePartitionId(REPLICA_MESSAGES_FACTORY.tablePartitionIdMessage()
                            .partitionId(2048)
                            .tableId(10_000)
                            .build())
                    .txCoordinatorId("node_foo_bar_123_yes")
                    .messageRowsToUpdate(map)
                    .build();
        } else {
            message = PARTITION_REPLICATION_MESSAGES_FACTORY.updateCommand()
                    .txId(uuid)
                    .leaseStartTime(timestamp)
                    .safeTimeLong(timestamp)
                    .rowUuid(uuid)
                    .requiredCatalogVersion(10_000)
                    .tablePartitionId(REPLICA_MESSAGES_FACTORY.tablePartitionIdMessage()
                            .partitionId(2048)
                            .tableId(10_000)
                            .build())
                    .txCoordinatorId("node_foo_bar_123_yes")
                    .messageRowToUpdate(timedBinaryRowMessage)
                    .build();
        }

        messageBytes = MARSHALLER.marshall(message);
    }

    /**
     * Runs the benchmark.
     *
     * @param args args
     * @throws Exception if something goes wrong
     */
    public static void main(String[] args) throws Exception {
        Options build = new OptionsBuilder()
                // .addProfiler("gc")
                .include(UpdateCommandsMarshalingMicroBenchmark.class.getName() + ".*").build();

        new Runner(build).run();
    }

    @Benchmark
    public byte[] marshal() {
        return MARSHALLER.marshall(message);
    }

    // TODO Optimize it further. https://issues.apache.org/jira/browse/IGNITE-22559
    @Benchmark
    public Object unmarshal() {
        return MARSHALLER.unmarshall(messageBytes);
    }
}
