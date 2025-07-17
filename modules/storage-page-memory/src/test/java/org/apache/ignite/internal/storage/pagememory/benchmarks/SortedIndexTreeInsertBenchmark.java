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

package org.apache.ignite.internal.storage.pagememory.benchmarks;

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.configuration.VolatileDataRegionConfiguration;
import org.apache.ignite.internal.pagememory.freelist.FreeListImpl;
import org.apache.ignite.internal.pagememory.inmemory.VolatilePageMemory;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor.StorageSortedIndexColumnDescriptor;
import org.apache.ignite.internal.storage.pagememory.index.freelist.IndexColumns;
import org.apache.ignite.internal.storage.pagememory.index.sorted.SortedIndexRow;
import org.apache.ignite.internal.storage.pagememory.index.sorted.SortedIndexTree;
import org.apache.ignite.internal.type.NativeType;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.Constants;
import org.apache.ignite.internal.util.OffheapReadWriteLock;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * A micro-benchmark for sorted index tree in a volatile data region.
 *
 * <p>In this benchmark the warmup must be much longer than the measurement. This is because the insertion duration correlates with a tree
 * height, which grows logarithmically with the number of inserted rows. Logarithm starts to grow slowly only after a certain threshold, but
 * before that it grows very quickly. We want to exhaust that growth before we start measuring the performance.
 */
@Warmup(iterations = 20, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Fork(1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class SortedIndexTreeInsertBenchmark {
    /** Size of a data region. Should be large enough to fit all the data in this benchmark. */
    private static final long REGION_SIZE = 4L * Constants.GiB;

    /** Page size. We may want to make it configurable in the future. Let's use a more performant 4Kb for now. */
    private static final int PAGE_SIZE = 4 * Constants.KiB;

    /** Group ID constant for the benchmark. Could be anything. */
    private static final int GROUP_ID = 1;
    /** Partition ID constant for the benchmark. Could be anything. */
    private static final int PARTITION_ID = 0;
    /** Index ID constant for the benchmark. Could be anything. */
    private static final int INDEX_ID = 1;

    /** Some fake row ID for benchmark. Reused in all operations, because allocating a new one every time is slow. */
    private static final RowId ROW_ID = new RowId(PARTITION_ID);

    /** An instance of {@link Random}. */
    private static final Random RANDOM = new Random(System.currentTimeMillis());

    /** {@link PageMemory} instance. */
    private VolatilePageMemory volatilePageMemory;

    /** Benchmark parameterization. We want to measure many different index columns descriptors. The list will eventually be expanded. */
    @Param({"LONG", "STRING_16"})
    public IndexDescriptorParam columnTypes;

    /** Benchmark parameter class. */
    public enum IndexDescriptorParam {
        /** A single column index with a long value. */
        LONG(List.of(descriptor(0, NativeTypes.INT64)), SortedIndexTreeInsertBenchmark::newLongTuple),
        /** A single column index with a string value, which will have only 16-character values. */
        STRING_16(List.of(descriptor(0, NativeTypes.STRING)), SortedIndexTreeInsertBenchmark::newString16Tuple);

        private final List<StorageSortedIndexColumnDescriptor> columnDescriptors;
        private final Supplier<ByteBuffer> tupleFactory;

        IndexDescriptorParam(List<StorageSortedIndexColumnDescriptor> columnDescriptors, Supplier<ByteBuffer> tupleFactory) {
            this.columnDescriptors = columnDescriptors;
            this.tupleFactory = tupleFactory;
        }

        /**
         * Returns the list of column descriptors for the index.
         */
        List<StorageSortedIndexColumnDescriptor> columnDescriptors() {
            return columnDescriptors;
        }

        /**
         * Creates a new tuple the corresponds to the index descriptor.
         */
        ByteBuffer createNewTuple() {
            return tupleFactory.get();
        }
    }

    /** An instance of {@link SortedIndexTree}. */
    private SortedIndexTree sortedIndexTree;

    /**
     * Initializes the benchmark state.
     */
    @Setup
    public void setup() throws Exception {
        var ioRegistry = new PageIoRegistry();
        ioRegistry.loadFromServiceLoader();

        volatilePageMemory = new VolatilePageMemory(
                VolatileDataRegionConfiguration.builder().pageSize(PAGE_SIZE).initSize(REGION_SIZE).maxSize(REGION_SIZE).build(),
                ioRegistry,
                new OffheapReadWriteLock(OffheapReadWriteLock.DEFAULT_CONCURRENCY_LEVEL)
        );

        volatilePageMemory.start();

        var freeList = new FreeListImpl(
                "freeList",
                GROUP_ID,
                PARTITION_ID,
                volatilePageMemory,
                volatilePageMemory.allocatePageNoReuse(GROUP_ID, PARTITION_ID, FLAG_AUX),
                true,
                null
        );

        sortedIndexTree = SortedIndexTree.createNew(
                GROUP_ID,
                "sortedIndex",
                PARTITION_ID,
                volatilePageMemory,
                new AtomicLong(),
                volatilePageMemory.allocatePageNoReuse(GROUP_ID, PARTITION_ID, FLAG_AUX),
                freeList,
                new StorageSortedIndexDescriptor(INDEX_ID, columnTypes.columnDescriptors(), false)
        );
    }

    /**
     * Invalidates the benchmark state.
     */
    @TearDown
    public void tearDown() throws Exception {
        volatilePageMemory.stop(true);
    }

    /**
     * Checks the performance of inserting a new row into the sorted index tree.
     */
    @Benchmark
    public void putx() throws Exception {
        ByteBuffer buffer = columnTypes.createNewTuple();

        sortedIndexTree.putx(new SortedIndexRow(new IndexColumns(PARTITION_ID, buffer), ROW_ID));
    }

    private static StorageSortedIndexColumnDescriptor descriptor(int i, NativeType nativeType) {
        return new StorageSortedIndexColumnDescriptor("col" + i, nativeType, false, true, true);
    }

    private static ByteBuffer newLongTuple() {
        long longValue = RANDOM.nextLong() | Long.MIN_VALUE;

        return new BinaryTupleBuilder(1, 10, true).appendLong(longValue).build();
    }

    private static ByteBuffer newString16Tuple() {
        String stringValue = Long.toHexString(RANDOM.nextLong() | Long.MIN_VALUE);

        return new BinaryTupleBuilder(1, 18, true).appendString(stringValue).build();
    }

    /**
     * Runs the benchmark.
     *
     * @param args args
     * @throws Exception if something goes wrong
     */
    public static void main(String[] args) throws Exception {
        Options build = new OptionsBuilder()
                .include(SortedIndexTreeInsertBenchmark.class.getName() + ".*").build();

        new Runner(build).run();
    }
}
