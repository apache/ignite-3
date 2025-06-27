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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.testframework.TestIgnitionManager.PRODUCTION_CLUSTER_CONFIG_STRING;
import static org.openjdk.jmh.annotations.Mode.AverageTime;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.catalog.definitions.ZoneDefinition;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * Benchmark that measures creating a new empty distribution zone.
 * The word `empty` means that the zone has no tables.
 */
@Fork(1)
@State(Scope.Benchmark)
public class CreatingDistributionZoneBenchmark extends AbstractMultiNodeBenchmark {
    @Param({"3"})
    private int clusterSize;

    @Param({"1", "8"})
    private int partitionCount;

    @Param({"1", "3"})
    private int replicaCount;

    @Param({"true", "false"})
    private boolean tinySchemaSyncWaits;

    /** Distribution zones counter. */
    private final AtomicInteger cnt = new AtomicInteger();

    @Override
    protected String clusterConfiguration() {
        if (tinySchemaSyncWaits) {
            return super.clusterConfiguration();
        } else {
            // Return a magic string that explicitly requests production defaults.
            return PRODUCTION_CLUSTER_CONFIG_STRING;
        }
    }

    @Override
    protected int nodes() {
        return clusterSize;
    }

    @Override
    protected int partitionCount() {
        return partitionCount;
    }

    @Override
    protected int replicaCount() {
        return replicaCount;
    }

    @Override
    protected void createDistributionZoneOnStartup() {
        // There is no need to create a zone on start-up.
    }

    @Override
    protected void createTablesOnStartup() {
        // There is no need to create a table on start-up.
    }

    /**
     * Measures creating a new empty distribution zone. The word `empty` means that the zone has no tables.
     */
    @Benchmark
    @Threads(1)
    @Warmup(iterations = 5, time = 5)
    @Measurement(iterations = 5, time = 5)
    @BenchmarkMode(AverageTime)
    @OutputTimeUnit(MILLISECONDS)
    public void createEmptyDistributionZone() {
        ZoneDefinition zone = ZoneDefinition.builder("zone_test_" + cnt.incrementAndGet())
                .partitions(partitionCount())
                .replicas(replicaCount())
                .storageProfiles(DEFAULT_STORAGE_PROFILE)
                .build();

        publicIgnite.catalog().createZone(zone);
    }

    /**
     * Benchmark's entry point.
     */
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(".*" + CreatingDistributionZoneBenchmark.class.getSimpleName() + ".*")
                .build();

        new Runner(opt).run();
    }
}
