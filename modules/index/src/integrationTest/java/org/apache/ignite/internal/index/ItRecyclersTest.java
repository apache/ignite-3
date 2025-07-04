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

package org.apache.ignite.internal.index;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.lang.Thread.State;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest.Person;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.raft.jraft.core.Replicator;
import org.apache.ignite.raft.jraft.util.ByteBufferCollector;
import org.apache.ignite.raft.jraft.util.Recyclers;
import org.apache.ignite.raft.jraft.util.Recyclers.DefaultHandle;
import org.apache.ignite.raft.jraft.util.Recyclers.Stack;
import org.apache.ignite.raft.jraft.util.Recyclers.WeakOrderQueue;
import org.apache.ignite.raft.jraft.util.RecyclersHandler;
import org.apache.ignite.raft.jraft.util.RecyclersHandlerOrigin;
import org.apache.ignite.raft.jraft.util.RecyclersHandlerSharedQueueOnly;
import org.apache.ignite.raft.jraft.util.RecyclersHandlerTwoQueue;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Integration test for testing optimization of {@link Recyclers}. */
@Timeout(value = 30, unit = TimeUnit.MINUTES)
public class ItRecyclersTest extends ClusterPerTestIntegrationTest {
    private static final String ZONE_NAME = "ZONE_TABLE";
    private static final String TABLE_NAME = "TEST_TABLE";

    private String testMethodName;

    @BeforeEach
    void setUp(TestInfo testInfo) {
        testMethodName = format("{}#{}", getClass().getSimpleName(), testInfo.getTestMethod().get().getName());
    }

    @Override
    protected int initialNodes() {
        return 3;
    }

    @Override
    protected boolean startClusterInBeforeEach() {
        return false;
    }

    private static Stream<Arguments> testArguments() {
        var arguments = new ArrayList<Arguments>();

        List<RecyclersHandler> recyclersHandlers = List.of(
                RecyclersHandlerOrigin.INSTANCE,
                RecyclersHandlerTwoQueue.INSTANCE,
                RecyclersHandlerSharedQueueOnly.INSTANCE
        );

        for (RecyclersHandler recyclersHandler : recyclersHandlers) {
            arguments.add(Arguments.arguments(recyclersHandler, 1, 1, 1_000));
            arguments.add(Arguments.arguments(recyclersHandler, 1, 25, 10_000));

            arguments.add(Arguments.arguments(recyclersHandler, 10, 1, 1_000));
            arguments.add(Arguments.arguments(recyclersHandler, 10, 25, 10_000));

            arguments.add(Arguments.arguments(recyclersHandler, 20, 1, 1_000));
            arguments.add(Arguments.arguments(recyclersHandler, 20, 25, 10_000));
        }

        return arguments.stream();
    }

    private static Stream<Arguments> test1Arguments() {
        var arguments = new ArrayList<Arguments>();

        for (boolean useSharedByteBuffers : new boolean[] {false, true}) {
            arguments.add(Arguments.arguments(useSharedByteBuffers, 1, 1, 1_000));
            arguments.add(Arguments.arguments(useSharedByteBuffers, 1, 25, 10_000));

            arguments.add(Arguments.arguments(useSharedByteBuffers, 10, 1, 1_000));
            arguments.add(Arguments.arguments(useSharedByteBuffers, 10, 25, 10_000));

            arguments.add(Arguments.arguments(useSharedByteBuffers, 20, 1, 1_000));
            arguments.add(Arguments.arguments(useSharedByteBuffers, 20, 25, 10_000));
        }

        return arguments.stream();
    }

    @AfterAll
    static void afterAll() {
        Recyclers.setRecyclersHandler(RecyclersHandlerOrigin.INSTANCE);
        Replicator.useSharedByteBuffers(false);
    }

    @ParameterizedTest(name = "useSharedBB={0}, tableCount={1}, perTablePartitionCount={2}, perTableInsertCount={3}")
    @MethodSource("test1Arguments")
    void test1(boolean useSharedByteBuffers, int tableCount, int partitionCount, int insertCount) {
        Replicator.useSharedByteBuffers(useSharedByteBuffers);

        test(RecyclersHandlerOrigin.INSTANCE, tableCount, partitionCount, insertCount);
    }

    @Disabled("https://issues.apache.org/jira/browse/IGNITE-25686")
    @ParameterizedTest(name = "h={0}, tableCount={1}, perTablePartitionCount={2}, perTableInsertCount={3}")
    @MethodSource("testArguments")
    void test(RecyclersHandler handler, int tableCount, int partitionCount, int insertCount) {
        long startNanos = System.nanoTime();

        int batchSize = 250;
        int nameLength = 20_000;

        String type = handler.name();

        Recyclers.setRecyclersHandler(handler);

        startAndInitCluster();

        log.info(
                ">>>>> {} before create tables: "
                        + "[type={}, tableCount={}, partitionCount={}, totalPartitionCount={}, replicas={}]",
                testMethodName, type, tableCount, partitionCount, (tableCount * partitionCount), initialNodes()
        );

        for (int i = 0; i < tableCount; i++) {
            createZoneAndTable(ZONE_NAME, TABLE_NAME + i, partitionCount, initialNodes());
        }

        log.info(">>>>> {} after create tables: ", testMethodName, type);

        var batchInserter = new TableBatchInserter(batchSize, type);

        for (int ti = 0; ti < tableCount; ti++) {
            batchInserter.tableName(TABLE_NAME + ti);

            for (int pi = 0; pi < insertCount; pi++) {
                batchInserter.add(new Person(pi, bigPersonName(nameLength, pi), pi));

                batchInserter.insertIfFilled();
            }

            batchInserter.insertIfNotEmpty();
        }

        log.info(">>>>> {} after fill tables: type", testMethodName, type);

        String nodeName = cluster.runningNodes()
                .map(Ignite::name)
                .findFirst()
                .orElseThrow(() -> new AssertionError("No node nome"));

        log.info(">>>>> {} node name: [type={}, name={}]", testMethodName, type, nodeName);

        log.info(
                ">>>>> {} newHandleCount by stack ordered: [type={}, info={}]",
                testMethodName, type, newHandleCountByStackOrdered(byteBufferCollectorOnlyForNodeStackFilter(nodeName))
        );

        log.info(
                ">>>>> {} weakOrderQueues by stack ordered: [type={}, info={}]",
                testMethodName, type, weakOrderQueuesByStackOrdered(byteBufferCollectorOnlyForNodeStackFilter(nodeName))
        );

        log.info(
                ">>>>> {} stacks size: [type={}, info={}]",
                testMethodName, type, stackSizesOrdered(byteBufferCollectorOnlyForNodeStackFilter(nodeName))
        );

        log.info(
                ">>>>> {} weakOrderQueues size: [type={}, info={}]",
                testMethodName, type, weakOrderQueuesSize(byteBufferCollectorOnlyForNodeQueueFilter(nodeName))
        );

        log.info(
                ">>>>> {} defaultHandles size: [type={}, info={}]",
                testMethodName, type, defaultHandlesSize(byteBufferCollectorOnlyForNodeHandleFilter(nodeName))
        );

        log.info(
                ">>>>> {} ByteBufferCollector sizes ordered: [type={}, info={}]",
                testMethodName, type, byteBufferCollectorSizes(nodeName, false)
        );

        log.info(
                ">>>>> {} ByteBufferCollector sizes ordered shared only: [type={}, info={}]",
                testMethodName, type, byteBufferCollectorSizes(nodeName, true)
        );

        log.info(
                ">>>>> {} ByteBufferCollector queses sizes ordered shared only: [type={}, info={}]",
                testMethodName, type, byteBufferCollectorQueueSizes(nodeName)
        );

        Duration duration = Duration.ofNanos(System.nanoTime() - startNanos);

        log.info(
                ">>>>> {} finish: "
                        + "[type={}, tableCount={}, partitionCount={}, totalPartitionCount={}, replicas={}, duration={}]",
                testMethodName, type, tableCount, partitionCount, (tableCount * partitionCount), initialNodes(), duration
        );
    }

    private void createZoneAndTable(String zoneName, String tableName, int replicas, int partitions) {
        executeSql(format(
                "CREATE ZONE IF NOT EXISTS {} (REPLICAS {}, PARTITIONS {}) STORAGE PROFILES ['{}'];",
                zoneName, replicas, partitions, DEFAULT_STORAGE_PROFILE
        ));

        executeSql(format(
                "CREATE TABLE IF NOT EXISTS {} (id INT PRIMARY KEY, name VARCHAR, salary DOUBLE) ZONE \"{}\"",
                tableName, zoneName
        ));
    }

    private void insertPeople(String tableName, Person... people) {
        Ignite node = cluster.node(0);

        Transaction tx = node.transactions().begin();

        String insertStmt = format("INSERT INTO {} (ID, NAME, SALARY) VALUES (?, ?, ?)", tableName);

        for (Person person : people) {
            ClusterPerClassIntegrationTest.sql(node, tx, null, null, insertStmt, person.id, person.name, person.salary);
        }

        tx.commit();
    }

    private static String bigPersonName(int length, int personIndex) {
        var sb = new StringBuilder(length + 100);

        for (int i = 0; i < length; i++) {
            sb.append('a');
        }

        return sb.append(personIndex).toString();
    }

    private static String byteBufferCollectorQueueSizes(String nodeName) {
        List<ByteBufferCollector> collect = Replicator.BYTE_BUFFER_COLLECTORS_BY_NODE_NAME.entrySet().stream()
                .filter(e -> e.getKey().contains(nodeName))
                .map(Entry::getValue)
                .flatMap(Collection::stream)
                .sorted(Comparator.comparingInt(ByteBufferCollector::capacity).reversed())
                .collect(toList());

        return byteBufferColletorsSummury(collect);
    }

    private static String byteBufferCollectorSizes(String nodeName, boolean sharedOnly) {
        Stream<ByteBufferCollector> stream0 = Recyclers.DEFAULT_HANDLES.values().stream()
                .flatMap(Collection::stream)
                .filter(h -> h.value instanceof ByteBufferCollector)
                .filter(h -> h.stackOrigin.thread.getState() != State.TERMINATED)
                .filter(h -> h.stackOrigin.thread.getName().contains(nodeName))
                .map(h -> (ByteBufferCollector) h.value);

        Stream<ByteBufferCollector> stream1 = Replicator.BYTE_BUFFER_COLLECTORS.stream()
                .filter(c -> c.nodeName.contains(nodeName));

        List<ByteBufferCollector> collect = (sharedOnly ? stream1 : Stream.concat(stream0, stream1))
                .sorted(Comparator.comparingInt(ByteBufferCollector::capacity).reversed())
                .collect(toList());

        return byteBufferColletorsSummury(collect);
    }

    private static String byteBufferColletorsSummury(List<ByteBufferCollector> collect) {
        long totalCapacity = collect.stream()
                .mapToLong(ByteBufferCollector::capacity)
                .sum();

        Map<String, Long> groupedCapacities = collect.stream()
                .map(c -> "capacity=" + IgniteUtils.readableSize(c.capacity(), false))
                .collect(groupingBy(Function.identity(), LinkedHashMap::new, counting()));

        return "[totalCapacity=" + IgniteUtils.readableSize(totalCapacity, false)
                + ", capacityCount=" + collect.size()
                + ", capacities=" + groupedCapacities
                + ']';
    }

    private static long weakOrderQueuesSize(Predicate<WeakOrderQueue> filter) {
        return Recyclers.WEAK_ORDER_QUEUES.values().stream()
                .flatMap(Collection::stream)
                .filter(filter)
                .count();
    }

    private static long defaultHandlesSize(Predicate<DefaultHandle> filter) {
        return Recyclers.DEFAULT_HANDLES.values().stream()
                .flatMap(Collection::stream)
                .filter(filter)
                .count();
    }

    private Predicate<DefaultHandle> byteBufferCollectorOnlyForNodeHandleFilter(String nodeName) {
        return h -> byteBufferCollectorOnlyForNodeStackFilter(nodeName).test(h.stackOrigin);
    }

    private Predicate<WeakOrderQueue> byteBufferCollectorOnlyForNodeQueueFilter(String nodeName) {
        return q -> byteBufferCollectorOnlyForNodeStackFilter(nodeName).test(q.stack);
    }

    private static String newHandleCountByStackOrdered(Predicate<Stack<?>> filter) {
        return countByStackOrdered(filter, CountByStack::newHandleCount);
    }

    private static String weakOrderQueuesByStackOrdered(Predicate<Stack<?>> filter) {
        return countByStackOrdered(filter, CountByStack::newWeakOrderQueueCount);
    }

    private static String stackSizesOrdered(Predicate<Stack<?>> filter) {
        return countByStackOrdered(filter, CountByStack::stackSize);
    }

    private static String countByStackOrdered(
            Predicate<Stack<?>> filter,
            Function<Stack<?>, CountByStack> toCountByStackFunction
    ) {
        Map<String, List<CountByStack>> byParentName = Recyclers.STACKS.stream()
                .filter(filter)
                .map(toCountByStackFunction)
                .collect(groupingBy(o -> o.parentName));

        var res = new HashMap<String, CollectionCountByStack>();

        byParentName.forEach((s, list) -> res.put(s, CollectionCountByStack.toSortedList(list)));

        return res.toString();
    }

    private static Predicate<Stack<?>> byteBufferCollectorOnlyStackFilter() {
        return s -> s.thread.getState() != State.TERMINATED && s.parent.getClass().getName().contains("ByteBufferCollector");
    }

    private static Predicate<Stack<?>> byteBufferCollectorOnlyForNodeStackFilter(String nodeName) {
        return byteBufferCollectorOnlyStackFilter().and(s -> s.thread.getName().contains(nodeName));
    }

    private final class TableBatchInserter {
        private final int bathSize;

        private final String type;

        private final List<Person> batch;

        private String tableName;

        private int batchCount;

        private int insertCount;

        private int insertTotalCount;

        private TableBatchInserter(int batchSize, String type) {
            this.bathSize = batchSize;
            this.type = type;

            batch = new ArrayList<>(batchSize);
        }

        private void add(Person p) {
            batch.add(p);
        }

        private void tableName(String tableName) {
            this.tableName = tableName;

            insertCount = 0;
        }

        private void insertIfFilled() {
            if (batch.size() == bathSize) {
                insertIfNotEmpty();
            }
        }

        private void insertIfNotEmpty() {
            if (!batch.isEmpty()) {
                insertPeople(tableName, batch.toArray(Person[]::new));

                insertCount += batch.size();
                insertTotalCount += batch.size();

                batch.clear();

                if (++batchCount % 10 == 0) {
                    log.info(
                            ">>>>> {} insert batch to table: "
                                    + "[type={}, tableName={}, insertCount={}, insertTotalCount={}]",
                            testMethodName, type, tableName, insertCount, insertTotalCount
                    );
                }
            }
        }
    }

    private static final class CountByStack implements Comparable<CountByStack> {
        private final long count;

        private final String parentName;

        private final String threadName;

        private CountByStack(Stack<?> stack, long count) {
            this.count = count;
            parentName = stack.parent.getClass().getName();
            threadName = stack.thread.getName();
        }

        static CountByStack newHandleCount(Stack<?> stack) {
            return new CountByStack(stack, stack.newHandleCount.get());
        }

        static CountByStack newWeakOrderQueueCount(Stack<?> stack) {
            return new CountByStack(stack, stack.newWeakOrderQueueCount.get());
        }

        static CountByStack stackSize(Stack<?> stack) {
            return new CountByStack(stack, stack.size);
        }

        @Override
        public int compareTo(CountByStack o) {
            int cmp = Long.compare(count, o.count);

            if (cmp != 0) {
                return cmp;
            }

            cmp = o.parentName.compareTo(o.parentName);

            if (cmp != 0) {
                return cmp;
            }

            return o.threadName.compareTo(o.threadName);
        }

        @Override
        public String toString() {
            return  "[thread=" + threadName + ", count=" + count + "]";
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof CountByStack && compareTo((CountByStack) o) == 0;
        }

        @Override
        public int hashCode() {
            int result = (int) (count ^ (count >>> 32));
            result = 31 * result + parentName.hashCode();
            result = 31 * result + threadName.hashCode();
            return result;
        }
    }

    private static final class CollectionCountByStack {
        private final Collection<CountByStack> collection;

        private final long totalCount;

        private CollectionCountByStack(Collection<CountByStack> collection) {
            this.collection = collection;

            totalCount = collection.stream()
                    .mapToLong(o -> o.count)
                    .sum();
        }

        static CollectionCountByStack toSortedList(List<CountByStack> list) {
            list.sort(Comparator.reverseOrder());

            return new CollectionCountByStack(list);
        }

        @Override
        public String toString() {
            return "[totalCount=" + totalCount + ", collection=" + collection + "]";
        }
    }
}
