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

package org.apache.ignite.internal.sql.engine.exec.rel;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import it.unimi.dsi.fastutil.longs.Long2ObjectMaps;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.failure.handlers.StopNodeFailureHandler;
import org.apache.ignite.internal.lang.InternalTuple;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.schema.BinaryRowConverter;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.sql.engine.SqlQueryProcessor;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.QueryTaskExecutorImpl;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowBuilder;
import org.apache.ignite.internal.sql.engine.exec.TxAttributes;
import org.apache.ignite.internal.sql.engine.exec.mapping.FragmentDescription;
import org.apache.ignite.internal.sql.engine.framework.ArrayRowHandler;
import org.apache.ignite.internal.sql.engine.framework.NoOpTransaction;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.thread.StripedThreadPoolExecutor;
import org.apache.ignite.internal.util.Pair;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

/**
 * Base abstract class for testing SQL execution.
 */
public abstract class AbstractExecutionTest<T> extends IgniteAbstractTest {
    public static final Object[][] EMPTY = new Object[0][];

    private QueryTaskExecutorImpl taskExecutor;

    @BeforeEach
    public void beforeTest() {
        var failureProcessor = new FailureProcessor("no_node", new StopNodeFailureHandler());
        taskExecutor = new QueryTaskExecutorImpl("no_node", 4, failureProcessor);
        taskExecutor.start();
    }

    /**
     * After each test.
     */
    @AfterEach
    public void afterTest() {
        taskExecutor.stop();
    }

    protected abstract RowHandler<T> rowHandler();

    protected ExecutionContext<T> executionContext() {
        return executionContext(false);
    }

    protected ExecutionContext<T> executionContext(boolean withDelays) {
        if (withDelays) {
            StripedThreadPoolExecutor testExecutor = new IgniteTestStripedThreadPoolExecutor(8,
                    NamedThreadFactory.create("fake-test-node", "sqlTestExec", log),
                    false,
                    0);

            StripedThreadPoolExecutor stripedThreadPoolExecutor = IgniteTestUtils.getFieldValue(
                    taskExecutor,
                    QueryTaskExecutorImpl.class,
                    "stripedThreadPoolExecutor"
            );

            // change it once on startup
            if (!(stripedThreadPoolExecutor instanceof IgniteTestStripedThreadPoolExecutor)) {
                stripedThreadPoolExecutor.shutdown();

                IgniteTestUtils.setFieldValue(taskExecutor, "stripedThreadPoolExecutor", testExecutor);
            }
        }

        FragmentDescription fragmentDesc = getFragmentDescription();

        return new ExecutionContext<>(
                taskExecutor,
                UUID.randomUUID(),
                new ClusterNodeImpl("1", "fake-test-node", NetworkAddress.from("127.0.0.1:1111")),
                "fake-test-node",
                fragmentDesc,
                rowHandler(),
                Map.of(),
                TxAttributes.fromTx(new NoOpTransaction("fake-test-node")),
                SqlQueryProcessor.DEFAULT_TIME_ZONE_ID
        );
    }

    protected FragmentDescription getFragmentDescription() {
        return new FragmentDescription(0, true, Long2ObjectMaps.emptyMap(), null, null, null);
    }

    protected Object[] row(Object... fields) {
        return fields;
    }

    /** Task reordering executor. */
    private static class IgniteTestStripedThreadPoolExecutor extends StripedThreadPoolExecutor {
        final Deque<Pair<Runnable, Integer>> tasks = new ArrayDeque<>();

        /** Internal stop flag. */
        AtomicBoolean stop = new AtomicBoolean();

        /** Inner execution service. */
        ExecutorService exec = Executors.newWorkStealingPool();

        CompletableFuture fut;

        /** {@inheritDoc} */
        public IgniteTestStripedThreadPoolExecutor(
                int concurrentLvl,
                ThreadFactory threadFactory,
                boolean allowCoreThreadTimeOut,
                long keepAliveTime
        ) {
            super(concurrentLvl, threadFactory, allowCoreThreadTimeOut, keepAliveTime);

            fut = IgniteTestUtils.runAsync(() -> {
                while (!stop.get()) {
                    synchronized (tasks) {
                        while (tasks.isEmpty()) {
                            try {
                                tasks.wait();
                            } catch (InterruptedException e) {
                                // no op.
                            }
                        }

                        Pair<Runnable, Integer> r = tasks.pollFirst();

                        exec.execute(() -> {
                            LockSupport.parkNanos(ThreadLocalRandom.current().nextLong(0, 10_000));
                            super.execute(r.getFirst(), r.getSecond());
                        });

                        tasks.notifyAll();
                    }
                }
            });
        }

        /** {@inheritDoc} */
        @Override public void execute(Runnable task, int idx) {
            synchronized (tasks) {
                tasks.add(new Pair<>(task, idx));

                tasks.notifyAll();
            }
        }

        /** {@inheritDoc} */
        @Override public void shutdown() {
            stop.set(true);

            exec.shutdown();
            fut.cancel(true);

            super.shutdown();
        }

        /** {@inheritDoc} */
        @Override public List<Runnable> shutdownNow() {
            stop.set(true);

            List<Runnable> runnables = exec.shutdownNow();
            fut.cancel(true);

            return Stream.concat(runnables.stream(), super.shutdownNow().stream()).collect(Collectors.toList());
        }
    }

    /**
     * Provides ability to generate test table data.
     */
    public static class TestTable implements Iterable<Object[]> {
        private int rowsCnt;

        private RelDataType rowType;

        private Function<Integer, Object>[] fieldCreators;

        TestTable(int rowsCnt, RelDataType rowType) {
            this(
                    rowsCnt,
                    rowType,
                    rowType.getFieldList().stream()
                            .map((Function<RelDataTypeField, Function<Integer, Object>>) (t) -> {
                                switch (t.getType().getSqlTypeName().getFamily()) {
                                    case NUMERIC:
                                        return TestTable::intField;

                                    case CHARACTER:
                                        return TestTable::stringField;

                                    default:
                                        assert false : "Not supported type for test: " + t;
                                        return null;
                                }
                            })
                            .collect(Collectors.toList()).toArray(new Function[rowType.getFieldCount()])
            );
        }

        TestTable(int rowsCnt, RelDataType rowType, Function<Integer, Object>... fieldCreators) {
            this.rowsCnt = rowsCnt;
            this.rowType = rowType;
            this.fieldCreators = fieldCreators;
        }

        private static Object field(Integer rowNum) {
            return "val_" + rowNum;
        }

        private static Object stringField(Integer rowNum) {
            return "val_" + rowNum;
        }

        private static Object intField(Integer rowNum) {
            return rowNum;
        }

        private Object[] createRow(int rowNum) {
            Object[] row = new Object[rowType.getFieldCount()];

            for (int i = 0; i < fieldCreators.length; ++i) {
                row[i] = fieldCreators[i].apply(rowNum);
            }

            return row;
        }

        /** {@inheritDoc} */
        @Override
        public Iterator<Object[]> iterator() {
            return new Iterator<>() {
                private int curRow;

                @Override
                public boolean hasNext() {
                    return curRow < rowsCnt;
                }

                @Override
                public Object[] next() {
                    return createRow(curRow++);
                }
            };
        }
    }

    /**
     * RootRewindable.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     */
    public static class RootRewindable<RowT> extends RootNode<RowT> {
        public RootRewindable(ExecutionContext<RowT> ctx) {
            super(ctx);
        }

        /** {@inheritDoc} */
        @Override
        protected void rewindInternal() {
            IgniteTestUtils.setFieldValue(this, RootNode.class, "waiting", 0);
            IgniteTestUtils.setFieldValue(this, RootNode.class, "closed", false);
        }

        /** {@inheritDoc} */
        @Override
        public void closeInternal() {
            // No-op
        }

        /** Remind count of rows. */
        public int rowsCount() {
            int cnt = 0;

            while (hasNext()) {
                next();

                cnt++;
            }

            rewind();

            return cnt;
        }

        /**
         * CloseRewindableRoot.
         * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
         */
        public void closeRewindableRoot() {
            super.closeInternal();
        }
    }

    static RowHandler.RowFactory<Object[]> rowFactory() {
        return new RowHandler.RowFactory<>() {
            @Override
            public RowHandler<Object[]> handler() {
                return ArrayRowHandler.INSTANCE;
            }

            @Override
            public RowBuilder<Object[]> rowBuilder() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Object[] create() {
                throw new AssertionError();
            }

            @Override
            public Object[] create(Object... fields) {
                return fields;
            }

            @Override
            public Object[] create(InternalTuple tuple) {
                throw new UnsupportedOperationException();
            }
        };
    }

    static TupleFactory tupleFactoryFromSchema(BinaryTupleSchema schema) {
        return new BinaryTupleFactory(schema);
    }

    @FunctionalInterface
    interface TupleFactory {
        InternalTuple create(Object... values);
    }

    private static class BinaryTupleFactory implements TupleFactory {
        private final BinaryTupleSchema schema;

        BinaryTupleFactory(BinaryTupleSchema schema) {
            this.schema = schema;
        }

        @Override
        public InternalTuple create(Object... values) {
            if (schema.elementCount() != values.length) {
                throw new IllegalArgumentException(
                        format("Expecting {} elements, but was {}", schema.elementCount(), values.length)
                );
            }

            BinaryTupleBuilder builder = new BinaryTupleBuilder(schema.elementCount());

            for (int i = 0; i < schema.elementCount(); i++) {
                BinaryRowConverter.appendValue(builder, schema.element(i), values[i]);
            }

            return new BinaryTuple(schema.elementCount(), builder.build());
        }
    }
}
