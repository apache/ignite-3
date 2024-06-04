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
import org.apache.ignite.internal.sql.engine.exec.mapping.bigcluster.BigClusterFactory;
import org.apache.ignite.internal.sql.engine.exec.mapping.smallcluster.AbstractTarget;
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

/** Mapping part benchmark.
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
    private ExecutionTarget allOfSmall;

    private ExecutionTarget oneOfBig;
    private ExecutionTarget someOfBig;
    private ExecutionTarget part1Big;
    private ExecutionTarget part2Big;
    private ExecutionTarget allOfBig;

    /** Prepare the plan of the query. */
    @Setup
    public void setUp() {
        List<String> nodes = List.of("n1", "n2", "n3", "n4", "n5");
        List<String> oneOfNodes = List.of("n1", "n3");
        List<NodeWithConsistencyToken> partNodes =
                List.of(new NodeWithConsistencyToken("n3", 1));

        ExecutionTargetFactory targetFactorySmall = new SmallClusterFactory(nodes);
        ExecutionTargetFactory targetFactoryBig = new BigClusterFactory(nodes);

        oneOfSmall = targetFactorySmall.oneOf(oneOfNodes);
        someOfSmall = targetFactorySmall.someOf(nodes);
        allOfSmall = targetFactorySmall.allOf(List.of("n3"));
        part1Small = targetFactorySmall.partitioned(partNodes);
        part2Small = targetFactorySmall.partitioned(partNodes);

        oneOfBig = targetFactoryBig.oneOf(oneOfNodes);
        someOfBig = targetFactoryBig.someOf(nodes);
        allOfBig = targetFactoryBig.allOf(List.of("n3"));
        part1Big = targetFactoryBig.partitioned(partNodes);
        part2Big = targetFactoryBig.partitioned(partNodes);
    }

    /** Measure small cluster mapping implementation. */
    @Benchmark
    public void benchSmall(Blackhole bh) throws ColocationMappingException {
        finalise(oneOfSmall.colocateWith(oneOfSmall), bh);
        finalise(oneOfSmall.colocateWith(someOfSmall), bh);
        finalise(oneOfSmall.colocateWith(allOfSmall), bh);
        finalise(oneOfSmall.colocateWith(part1Small), bh);

        finalise(someOfSmall.colocateWith(someOfSmall), bh);
        finalise(someOfSmall.colocateWith(part1Small), bh);
        finalise(someOfSmall.colocateWith(allOfSmall), bh);

        finalise(part1Small.colocateWith(part2Small), bh);
    }

    private static void finalise(ExecutionTarget target, Blackhole bh) {
        bh.consume(((AbstractTarget) target).finalise());
    }

    /** Measure huge cluster mapping implementation. */
    @Benchmark
    public void benchBig(Blackhole bh) throws ColocationMappingException {
        bh.consume(oneOfBig.colocateWith(oneOfBig));
        bh.consume(oneOfBig.colocateWith(someOfBig));
        bh.consume(oneOfBig.colocateWith(allOfBig));
        bh.consume(oneOfBig.colocateWith(part1Big));

        bh.consume(someOfBig.colocateWith(someOfBig));
        bh.consume(someOfBig.colocateWith(part1Big));
        bh.consume(someOfBig.colocateWith(allOfBig));

        bh.consume(part1Big.colocateWith(part2Big));
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
