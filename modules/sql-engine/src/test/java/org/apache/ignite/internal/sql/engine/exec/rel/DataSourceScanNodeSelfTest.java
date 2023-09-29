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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.BinaryTupleSchema.Element;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.row.InternalTuple;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.ScannableDataSource;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler.RowWrapper;
import org.apache.ignite.internal.sql.engine.exec.row.BaseTypeSpec;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.sql.engine.exec.row.TypeSpec;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/** Tests to verify {@link DataSourceScanNode}. */
@SuppressWarnings("resource")
public class DataSourceScanNodeSelfTest extends AbstractExecutionTest<RowWrapper> {
    private static final RowSchema ROW_SCHEMA = RowSchema.builder()
            .addField(NativeTypes.INT32)
            .addField(NativeTypes.INT64)
            .addField(NativeTypes.INT8)
            .addField(NativeTypes.stringOf(64))
            .addField(NativeTypes.UUID)
            .build();

    private static final BinaryTupleSchema TUPLE_SCHEMA = fromRowSchema(ROW_SCHEMA);

    private static final TupleFactory TUPLE_FACTORY = tupleFactoryFromSchema(TUPLE_SCHEMA);

    private static final Object[][] ROWS = {
            {111, 11L, (byte) 1, "1111", new UUID(0L, 1L)},
            {222, 22L, (byte) 2, "2222", new UUID(0L, 2L)},
            {333, 33L, (byte) 3, "3333", new UUID(0L, 3L)},
            {444, 44L, (byte) 4, "4444", new UUID(0L, 4L)},
            {555, 55L, (byte) 5, "5555", new UUID(0L, 5L)},
    };

    @Test
    void simpleTest() {
        ExecutionContext<RowWrapper> context = executionContext();
        List<RowWrapper> rows = initScanAndGetResults(context, null, null, null);

        assertThat(rows, notNullValue());

        List<String> expectedRows = List.of(
                "Row[111, 11, 1, 1111, 00000000-0000-0000-0000-000000000001]",
                "Row[222, 22, 2, 2222, 00000000-0000-0000-0000-000000000002]",
                "Row[333, 33, 3, 3333, 00000000-0000-0000-0000-000000000003]",
                "Row[444, 44, 4, 4444, 00000000-0000-0000-0000-000000000004]",
                "Row[555, 55, 5, 5555, 00000000-0000-0000-0000-000000000005]"
        );

        RowHandler<RowWrapper> handler = context.rowHandler();

        List<String> actualRows = rows.stream().map(handler::toString).collect(Collectors.toList());

        assertThat(actualRows, equalTo(expectedRows));
    }

    @Test
    void scanWithRequiredFields() {
        ExecutionContext<RowWrapper> context = executionContext();
        RowHandler<RowWrapper> handler = context.rowHandler();
        List<RowWrapper> rows = initScanAndGetResults(context, null, null, ImmutableBitSet.of(1, 3, 4).toBitSet());

        assertThat(rows, notNullValue());

        List<String> expectedRows = List.of(
                "Row[11, 1111, 00000000-0000-0000-0000-000000000001]",
                "Row[22, 2222, 00000000-0000-0000-0000-000000000002]",
                "Row[33, 3333, 00000000-0000-0000-0000-000000000003]",
                "Row[44, 4444, 00000000-0000-0000-0000-000000000004]",
                "Row[55, 5555, 00000000-0000-0000-0000-000000000005]"
        );

        List<String> actualRows = rows.stream().map(handler::toString).collect(Collectors.toList());

        assertThat(actualRows, equalTo(expectedRows));
    }

    @Test
    @SuppressWarnings("DataFlowIssue")
    void scanWithProjection() {
        ExecutionContext<RowWrapper> context = executionContext();
        RowHandler<RowWrapper> handler = context.rowHandler();
        RowFactory<RowWrapper> factory = handler.factory(ROW_SCHEMA);

        Function<RowWrapper, RowWrapper> doubleFirstColumnProjection = row -> {
            int size = handler.columnCount(row);

            Object[] values = new Object[size];

            values[0] = (Integer) handler.get(0, row) * 2;

            for (int i = 1; i < size; i++) {
                values[i] = handler.get(i, row);
            }

            return factory.create(values);
        };

        List<RowWrapper> rows = initScanAndGetResults(context, null, doubleFirstColumnProjection, null);

        assertThat(rows, notNullValue());

        List<String> expectedRows = List.of(
                "Row[222, 11, 1, 1111, 00000000-0000-0000-0000-000000000001]",
                "Row[444, 22, 2, 2222, 00000000-0000-0000-0000-000000000002]",
                "Row[666, 33, 3, 3333, 00000000-0000-0000-0000-000000000003]",
                "Row[888, 44, 4, 4444, 00000000-0000-0000-0000-000000000004]",
                "Row[1110, 55, 5, 5555, 00000000-0000-0000-0000-000000000005]"
        );

        List<String> actualRows = rows.stream().map(handler::toString).collect(Collectors.toList());

        assertThat(actualRows, equalTo(expectedRows));
    }

    @Test
    @SuppressWarnings("DataFlowIssue")
    void scanWithFilter() {
        ExecutionContext<RowWrapper> context = executionContext();
        RowHandler<RowWrapper> handler = context.rowHandler();

        Predicate<RowWrapper> onlyEven = row -> ((Integer) handler.get(0, row)) % 2 == 0;

        List<RowWrapper> rows = initScanAndGetResults(context, onlyEven, null, null);

        assertThat(rows, notNullValue());

        List<String> expectedRows = List.of(
                "Row[222, 22, 2, 2222, 00000000-0000-0000-0000-000000000002]",
                "Row[444, 44, 4, 4444, 00000000-0000-0000-0000-000000000004]"
        );

        List<String> actualRows = rows.stream().map(handler::toString).collect(Collectors.toList());

        assertThat(actualRows, equalTo(expectedRows));
    }

    @Test
    @SuppressWarnings("DataFlowIssue")
    void scanWithAllOptions() {
        ExecutionContext<RowWrapper> context = executionContext();
        RowHandler<RowWrapper> handler = context.rowHandler();

        BitSet requiredFields = ImmutableBitSet.of(1, 3, 4).toBitSet();

        RowFactory<RowWrapper> factory = handler.factory(project(ROW_SCHEMA, requiredFields.stream().toArray()));

        // predicate matching goes before projection transformation, thus this predicate is valid
        Predicate<RowWrapper> onlyEven = row -> ((Long) handler.get(0, row)) % 2 == 0;

        Function<RowWrapper, RowWrapper> doubleFirstColumnProjection = row -> {
            int size = handler.columnCount(row);

            Object[] values = new Object[size];

            values[0] = (Long) handler.get(0, row) * 2;

            for (int i = 1; i < size; i++) {
                values[i] = handler.get(i, row);
            }

            return factory.create(values);
        };

        List<RowWrapper> rows = initScanAndGetResults(context, onlyEven, doubleFirstColumnProjection, requiredFields);

        assertThat(rows, notNullValue());

        List<String> expectedRows = List.of(
                "Row[44, 2222, 00000000-0000-0000-0000-000000000002]",
                "Row[88, 4444, 00000000-0000-0000-0000-000000000004]"
        );

        List<String> actualRows = rows.stream().map(handler::toString).collect(Collectors.toList());

        assertThat(actualRows, equalTo(expectedRows));
    }

    @SuppressWarnings("DataFlowIssue")
    private static List<RowWrapper> initScanAndGetResults(
            ExecutionContext<RowWrapper> context,
            @Nullable Predicate<RowWrapper> predicate,
            @Nullable Function<RowWrapper, RowWrapper> projection,
            @Nullable BitSet requiredFields
    ) {
        RowHandler<RowWrapper> handler = context.rowHandler();
        RowFactory<RowWrapper> factory;
        if (requiredFields != null) {
            factory = handler.factory(project(ROW_SCHEMA, requiredFields.stream().toArray()));
        } else {
            factory = handler.factory(ROW_SCHEMA);
        }

        ScannableDataSource dataSource = new IterableDataSource(
                Stream.of(ROWS).map(TUPLE_FACTORY::create).collect(Collectors.toList())
        );
        DataSourceScanNode<RowWrapper> node = new DataSourceScanNode<>(
                context, factory, fromRowSchema(ROW_SCHEMA), dataSource, predicate, projection, requiredFields
        );

        DrainAllDownstream<RowWrapper> downstream = new DrainAllDownstream<>();

        node.onRegister(downstream);

        context.execute(() -> node.request(Integer.MAX_VALUE), node::onError);

        return await(downstream.completion);
    }

    static class IterableDataSource implements ScannableDataSource {
        private final Iterable<InternalTuple> iterable;

        IterableDataSource(Iterable<InternalTuple> iterable) {
            this.iterable = iterable;
        }

        @Override
        public Publisher<InternalTuple> scan() {
            Iterator<InternalTuple> it = iterable.iterator();

            return new Publisher<InternalTuple>() {
                @Override
                public void subscribe(Subscriber<? super InternalTuple> subscriber) {
                    Subscription subscription = new Subscription() {
                        @Override
                        public void request(long n) {
                            if (n <= 0) {
                                subscriber.onError(new IllegalArgumentException());

                                return;
                            }

                            while (n > 0 && it.hasNext()) {
                                subscriber.onNext(it.next());

                                n--;
                            }

                            if (n > 0) {
                                subscriber.onComplete();
                            }
                        }

                        @Override
                        public void cancel() {
                            // NO-OP
                        }
                    };

                    subscriber.onSubscribe(subscription);
                }
            };
        }
    }

    static class DrainAllDownstream<T> implements Downstream<T> {
        private final List<T> rows = new ArrayList<>();
        private final CompletableFuture<List<T>> completion = new CompletableFuture<>();

        @Override
        public void push(T row) {
            rows.add(row);
        }

        @Override
        public void end() throws Exception {
            completion.complete(rows);
        }

        @Override
        public void onError(Throwable e) {
            completion.completeExceptionally(e);
        }
    }

    @Override
    protected RowHandler<RowWrapper> rowHandler() {
        return SqlRowHandler.INSTANCE;
    }

    private static BinaryTupleSchema fromRowSchema(RowSchema schema) {
        Element[] elements = new Element[schema.fields().size()];

        int idx = 0;
        for (TypeSpec spec : schema.fields()) {
            assert spec instanceof BaseTypeSpec : spec;

            elements[idx++] = new Element(((BaseTypeSpec) spec).nativeType(), spec.isNullable());
        }

        return BinaryTupleSchema.create(elements);
    }

    private static RowSchema project(RowSchema schema, int[] projection) {
        RowSchema.Builder builder = RowSchema.builder();
        for (int i : projection) {
            builder.addField(schema.fields().get(i));
        }

        return builder.build();
    }
}
