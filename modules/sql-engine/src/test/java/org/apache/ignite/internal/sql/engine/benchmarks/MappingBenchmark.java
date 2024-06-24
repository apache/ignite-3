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

package org.apache.ignite.internal.sql.engine.benchmarks;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.sql.engine.exec.NodeWithConsistencyToken;
import org.apache.ignite.internal.sql.engine.exec.mapping.ColocationMappingException;
import org.apache.ignite.internal.sql.engine.exec.mapping.ExecutionTarget;
import org.apache.ignite.internal.sql.engine.exec.mapping.ExecutionTargetFactory;
import org.apache.ignite.internal.sql.engine.exec.mapping.largecluster.LargeClusterFactory;
import org.apache.ignite.internal.sql.engine.exec.mapping.smallcluster.SmallClusterFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Mapping part benchmark.
 *
 * <p>Note: seems more accurate results can be obtained after boosting disabling:
 * linux: echo "1" | sudo tee /sys/devices/system/cpu/intel_pstate/no_turbo
 */
@Warmup(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 1, timeUnit = TimeUnit.SECONDS)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
@State(Scope.Benchmark)
public class MappingBenchmark {
    private ExecutionTarget oneOfSmall;
    private ExecutionTarget someOfSmall;
    private ExecutionTarget part1Small;
    private ExecutionTarget part2Small;
    private ExecutionTarget part1SmallMore;
    private ExecutionTarget part2SmallMore;
    private ExecutionTarget allOfSmall;

    private ExecutionTarget oneOfBig;
    private ExecutionTarget someOfBig;
    private ExecutionTarget part1Big;
    private ExecutionTarget part2Big;
    private ExecutionTarget part1BigMore;
    private ExecutionTarget part2BigMore;
    private ExecutionTarget allOfBig;

    private ExecutionTargetFactory targetFactorySmall;
    private ExecutionTargetFactory targetFactoryLarge;

    /** Prepare the plan of the query. */
    @Setup
    public void setUp() {
        List<String> nodes = List.of("n1", "n2", "n3", "n4", "n5");
        List<String> oneOfNodes = List.of("n1", "n3");
        List<NodeWithConsistencyToken> partNodes =
                List.of(new NodeWithConsistencyToken("n3", 1));
        List<NodeWithConsistencyToken> partNodesMore =
                List.of(new NodeWithConsistencyToken("n3", 1),
                        new NodeWithConsistencyToken("n4", 2),
                        new NodeWithConsistencyToken("n5", 3));

        targetFactorySmall = new SmallClusterFactory(nodes);
        targetFactoryLarge = new LargeClusterFactory(nodes);

        oneOfSmall = targetFactorySmall.oneOf(oneOfNodes);
        someOfSmall = targetFactorySmall.someOf(nodes);
        allOfSmall = targetFactorySmall.allOf(List.of("n3"));
        part1Small = targetFactorySmall.partitioned(partNodes);
        part2Small = targetFactorySmall.partitioned(partNodes);
        part1SmallMore = targetFactorySmall.partitioned(partNodesMore);
        part2SmallMore = targetFactorySmall.partitioned(partNodesMore);

        oneOfBig = targetFactoryLarge.oneOf(oneOfNodes);
        someOfBig = targetFactoryLarge.someOf(nodes);
        allOfBig = targetFactoryLarge.allOf(List.of("n3"));
        part1Big = targetFactoryLarge.partitioned(partNodes);
        part2Big = targetFactoryLarge.partitioned(partNodes);
        part1BigMore = targetFactoryLarge.partitioned(partNodesMore);
        part2BigMore = targetFactoryLarge.partitioned(partNodesMore);
    }

    /** Measure small cluster mapping implementation. */
    @Benchmark
    public void benchSmall(Blackhole bh) throws ColocationMappingException {
        finalise(targetFactorySmall, oneOfSmall.colocateWith(oneOfSmall), bh);
        finalise(targetFactorySmall, oneOfSmall.colocateWith(someOfSmall), bh);
        finalise(targetFactorySmall, oneOfSmall.colocateWith(allOfSmall), bh);
        finalise(targetFactorySmall, oneOfSmall.colocateWith(part1Small), bh);

        finalise(targetFactorySmall, someOfSmall.colocateWith(someOfSmall), bh);
        finalise(targetFactorySmall, someOfSmall.colocateWith(part1Small), bh);
        finalise(targetFactorySmall, someOfSmall.colocateWith(allOfSmall), bh);

        finalise(targetFactorySmall, part1Small.colocateWith(part2Small), bh);
    }

    /** Measure small cluster mapping partitioned only implementation. */
    @Benchmark
    public void benchSmallPartitionedOnly(Blackhole bh) throws ColocationMappingException {
        finalise(targetFactorySmall, part1SmallMore.colocateWith(part2SmallMore), bh);
    }

    private static void finalise(ExecutionTargetFactory factory, ExecutionTarget target, Blackhole bh) {
        bh.consume(factory.resolveNodes(target));
    }

    /** Measure huge cluster mapping implementation. */
    @Benchmark
    public void benchBig(Blackhole bh) throws ColocationMappingException {
        finalise(targetFactoryLarge, oneOfBig.colocateWith(oneOfBig), bh);
        finalise(targetFactoryLarge, oneOfBig.colocateWith(someOfBig), bh);
        finalise(targetFactoryLarge, oneOfBig.colocateWith(allOfBig), bh);
        finalise(targetFactoryLarge, oneOfBig.colocateWith(part1Big), bh);

        finalise(targetFactoryLarge, someOfBig.colocateWith(someOfBig), bh);
        finalise(targetFactoryLarge, someOfBig.colocateWith(part1Big), bh);
        finalise(targetFactoryLarge, someOfBig.colocateWith(allOfBig), bh);

        finalise(targetFactoryLarge, part1Big.colocateWith(part2Big), bh);
    }

    /** Measure huge cluster mapping partitioned only implementation. */
    @Benchmark
    public void benchBigPartitionedOnly(Blackhole bh) throws ColocationMappingException {
        finalise(targetFactoryLarge, part1BigMore.colocateWith(part2BigMore), bh);
    }

    /**
     * Runs the benchmark.
     *
     * @param args args
     * @throws Exception if something goes wrong
     */
    public static void main(String[] args) throws Exception {
        Options build = new OptionsBuilder()
                .include(MappingBenchmark.class.getName())
                .build();

        new Runner(build).run();
    }
}