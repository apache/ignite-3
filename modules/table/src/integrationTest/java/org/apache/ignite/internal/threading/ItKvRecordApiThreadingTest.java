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

package org.apache.ignite.internal.threading;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.lang.IgniteSystemProperties;
import org.apache.ignite.internal.streamer.SimplePublisher;
import org.apache.ignite.internal.test.WatchListenerInhibitor;
import org.apache.ignite.internal.testframework.TestIgnitionManager;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.thread.IgniteThread;
import org.apache.ignite.internal.thread.PublicApiThreading;
import org.apache.ignite.sql.Session;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Enum;

@WithSystemProperty(key = IgniteSystemProperties.THREAD_ASSERTIONS_ENABLED, value = "false")
@SuppressWarnings("resource")
class ItKvRecordApiThreadingTest extends ClusterPerClassIntegrationTest {
    private static final String TABLE_NAME = "test";
    private static final int KEY = 1;

    private static final Record KEY_RECORD = new Record(1, "");

    @Override
    protected int initialNodes() {
        return 1;
    }

    @BeforeAll
    void createTable() {
        try (Session session = firstNode().sql().createSession()) {
            session.execute(null, "CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, val VARCHAR)");
        }
    }

    private static IgniteImpl firstNode() {
        return CLUSTER.node(0);
    }

    @BeforeEach
    void upsertRecord() {
        plainKeyValueView().put(null, KEY, "one");
    }

    private static KeyValueView<Integer, String> plainKeyValueView() {
        return testTable().keyValueView(Integer.class, String.class);
    }

    private static KeyValueView<Tuple, Tuple> binaryKeyValueView() {
        return testTable().keyValueView();
    }

    private static Table testTable() {
        return firstNode().tables().table(TABLE_NAME);
    }

    @SuppressWarnings("rawtypes")
    @CartesianTest
    void keyValueViewFuturesCompleteInContinuationsPool(
            @Enum KeyValueViewAsyncOperation operation,
            @Enum KeyValueViewKind kind
    ) {
        assumeTrue(kind.supportsGetNullable() || !operation.isGetNullable());

        KeyValueView tableView = kind.view();

        @SuppressWarnings("unchecked") CompletableFuture<Thread> completerFuture = forcingSwitchFromUserThread(
                () -> operation.executeOn(tableView, kind.context())
                        .thenApply(unused -> Thread.currentThread())
        );

        assertThat(completerFuture, willBe(commonPoolThread()));
    }

    private static Matcher<Object> commonPoolThread() {
        return both(hasProperty("name", startsWith("ForkJoinPool.commonPool-worker-")))
                .and(not(instanceOf(IgniteThread.class)));
    }

    private static <T> T forcingSwitchFromUserThread(Supplier<? extends T> action) {
        return WatchListenerInhibitor.withInhibition(firstNode(), () -> {
            waitForSchemaSyncRequiringWait();

            return action.get();
        });
    }

    private static void waitForSchemaSyncRequiringWait() {
        try {
            Thread.sleep(TestIgnitionManager.DEFAULT_DELAY_DURATION_MS + 1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new RuntimeException(e);
        }
    }

    private static KeyValueContext<Integer, String> plainKeyValueContext() {
        return new KeyValueContext<>(KEY, "one", "two");
    }

    private static KeyValueContext<Tuple, Tuple> binaryKeyValueContext() {
        return new KeyValueContext<>(Tuple.create().set("id", KEY), Tuple.create().set("val", "one"), Tuple.create().set("val", "two"));
    }

    @SuppressWarnings("rawtypes")
    @CartesianTest
    void keyValueViewFuturesFromInternalCallsAreNotResubmittedToContinuationsPool(
            @Enum KeyValueViewAsyncOperation operation,
            @Enum KeyValueViewKind kind
    ) {
        assumeTrue(kind.supportsGetNullable() || !operation.isGetNullable());

        KeyValueView tableView = kind.view();

        @SuppressWarnings("unchecked") CompletableFuture<Thread> completerFuture = forcingSwitchFromUserThread(
                () -> PublicApiThreading.doInternalCall(
                        () -> operation.executeOn(tableView, kind.context())
                                .thenApply(unused -> Thread.currentThread())
                )
        );

        assertThat(completerFuture, willBe(anIgniteThread()));
    }

    private static Matcher<Object> anIgniteThread() {
        return instanceOf(IgniteThread.class);
    }

    @SuppressWarnings("rawtypes")
    @CartesianTest
    void recordViewFuturesCompleteInContinuationsPool(
            @Enum RecordViewAsyncOperation operation,
            @Enum RecordViewKind kind
    ) {
        RecordView tableView = kind.view();

        @SuppressWarnings("unchecked") CompletableFuture<Thread> completerFuture = forcingSwitchFromUserThread(
                () -> operation.executeOn(tableView, kind.context())
                        .thenApply(unused -> Thread.currentThread())
        );

        assertThat(completerFuture, willBe(commonPoolThread()));
    }

    @SuppressWarnings("rawtypes")
    @CartesianTest
    void recordViewFuturesFromInternalCallsAreNotResubmittedToContinuationsPool(
            @Enum RecordViewAsyncOperation operation,
            @Enum RecordViewKind kind
    ) {
        RecordView tableView = kind.view();

        @SuppressWarnings("unchecked") CompletableFuture<Thread> completerFuture = forcingSwitchFromUserThread(
                () -> PublicApiThreading.doInternalCall(
                        () -> operation.executeOn(tableView, kind.context())
                                .thenApply(unused -> Thread.currentThread())
                )
        );

        assertThat(completerFuture, willBe(anIgniteThread()));
    }

    private static RecordView<Record> plainRecordView() {
        return testTable().recordView(Record.class);
    }

    private static RecordView<Tuple> binaryRecordView() {
        return testTable().recordView();
    }

    private static RecordContext<Record> plainRecordContext() {
        return new RecordContext<>(KEY_RECORD, new Record(KEY, "one"), new Record(KEY, "two"));
    }

    private static RecordContext<Tuple> binaryRecordContext() {
        return new RecordContext<>(KEY_RECORD.toKeyTuple(), new Record(KEY, "one").toFullTuple(), new Record(KEY, "two").toFullTuple());
    }

    private enum KeyValueViewAsyncOperation {
        GET_ASYNC((view, context) -> view.getAsync(null, context.key)),
        GET_NULLABLE_ASYNC((view, context) -> view.getNullableAsync(null, context.key)),
        GET_OR_DEFAULT_ASYNC((view, context) -> view.getOrDefaultAsync(null, context.key, context.anotherValue)),
        GET_ALL_ASYNC((view, context) -> view.getAllAsync(null, List.of(context.key))),
        CONTAINS_ASYNC((view, context) -> view.containsAsync(null, context.key)),
        PUT_ASYNC((view, context) -> view.putAsync(null, context.key, context.usualValue)),
        PUT_ALL_ASYNC((view, context) -> view.putAllAsync(null, Map.of(context.key, context.usualValue))),
        GET_AND_PUT_ASYNC((view, context) -> view.getAndPutAsync(null, context.key, context.usualValue)),
        GET_NULLABLE_AND_PUT_ASYNC((view, context) -> view.getNullableAndPutAsync(null, context.key, context.usualValue)),
        PUT_IF_ABSENT_ASYNC((view, context) -> view.putIfAbsentAsync(null, context.key, context.usualValue)),
        REMOVE_ASYNC((view, context) -> view.removeAsync(null, context.key)),
        REMOVE_EXACT_ASYNC((view, context) -> view.removeAsync(null, context.key, context.usualValue)),
        REMOVE_ALL_ASYNC((view, context) -> view.removeAllAsync(null, List.of(context.key))),
        GET_AND_REMOVE_ASYNC((view, context) -> view.getAndRemoveAsync(null, context.key)),
        GET_NULLABLE_AND_REMOVE_ASYNC((view, context) -> view.getNullableAndRemoveAsync(null, context.key)),
        REPLACE_ASYNC((view, context) -> view.replaceAsync(null, context.key, context.usualValue)),
        REPLACE_EXACT_ASYNC((view, context) -> view.replaceAsync(null, context.key, context.usualValue, context.anotherValue)),
        GET_AND_REPLACE_ASYNC((view, context) -> view.getAndReplaceAsync(null, context.key, context.usualValue)),
        GET_NULLABLE_AND_REPLACE_ASYNC((view, context) -> view.getNullableAndReplaceAsync(null, context.key, context.usualValue)),
        @SuppressWarnings({"rawtypes", "unchecked"})
        STREAM_DATA((view, context) -> {
            CompletableFuture<?> future;
            try (var publisher = new SimplePublisher()) {
                future = view.streamData(publisher, null);
                publisher.submit(Map.entry(context.key, context.usualValue));
            }
            return future;
        }),
        QUERY_ASYNC((view, context) -> view.queryAsync(null, null)),
        QUERY_ASYNC_WITH_INDEX_NAME((view, context) -> view.queryAsync(null, null, null)),
        QUERY_ASYNC_WITH_INDEX_NAME_AND_OPTS((view, context) -> view.queryAsync(null, null, null, null));

        private final BiFunction<KeyValueView<Object, Object>, KeyValueContext<Object, Object>, CompletableFuture<?>> action;

        KeyValueViewAsyncOperation(BiFunction<KeyValueView<Object, Object>, KeyValueContext<Object, Object>, CompletableFuture<?>> action) {
            this.action = action;
        }

        <K, V> CompletableFuture<?> executeOn(KeyValueView<K, V> tableView, KeyValueContext<K, V> context) {
            return action.apply((KeyValueView<Object, Object>) tableView, (KeyValueContext<Object, Object>) context);
        }

        boolean isGetNullable() {
            switch (this) {
                case GET_NULLABLE_ASYNC:
                case GET_NULLABLE_AND_PUT_ASYNC:
                case GET_NULLABLE_AND_REMOVE_ASYNC:
                case GET_NULLABLE_AND_REPLACE_ASYNC:
                    return true;
                default:
                    return false;
            }
        }
    }

    private static class KeyValueContext<K, V> {
        final K key;
        final V usualValue;
        final V anotherValue;

        private KeyValueContext(K key, V usualValue, V anotherValue) {
            this.key = key;
            this.usualValue = usualValue;
            this.anotherValue = anotherValue;
        }
    }

    @SuppressWarnings("rawtypes")
    private enum KeyValueViewKind {
        PLAIN, BINARY;

        KeyValueView view() {
            return this == PLAIN ? plainKeyValueView() : binaryKeyValueView();
        }

        KeyValueContext context() {
            return this == PLAIN ? plainKeyValueContext() : binaryKeyValueContext();
        }

        boolean supportsGetNullable() {
            return this == PLAIN;
        }
    }

    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private static class Record {
        private int id;
        private String val;

        private Record() {
        }

        private Record(int id, String val) {
            this.id = id;
            this.val = val;
        }

        Tuple toKeyTuple() {
            return Tuple.create().set("id", id);
        }

        Tuple toFullTuple() {
            return Tuple.create().set("id", id).set("val", val);
        }
    }

    private enum RecordViewAsyncOperation {
        GET_ASYNC((view, context) -> view.getAsync(null, context.keyRecord)),
        GET_ALL_ASYNC((view, context) -> view.getAllAsync(null, List.of(context.keyRecord))),
        CONTAINS_ASYNC((view, context) -> view.containsAsync(null, context.keyRecord)),
        UPSERT_ASYNC((view, context) -> view.upsertAsync(null, context.fullRecord)),
        UPSERT_ALL_ASYNC((view, context) -> view.upsertAllAsync(null, List.of(context.fullRecord))),
        GET_AND_UPSERT_ASYNC((view, context) -> view.getAndUpsertAsync(null, context.fullRecord)),
        INSERT_ASYNC((view, context) -> view.insertAsync(null, context.fullRecord)),
        INSERT_ALL_ASYNC((view, context) -> view.insertAllAsync(null, List.of(context.fullRecord))),
        REPLACE_ASYNC((view, context) -> view.replaceAsync(null, context.fullRecord)),
        REPLACE_EXACT_ASYNC((view, context) -> view.replaceAsync(null, context.fullRecord, context.anotherFullRecord)),
        GET_AND_REPLACE_ASYNC((view, context) -> view.getAndReplaceAsync(null, context.keyRecord)),
        DELETE_ASYNC((view, context) -> view.deleteAsync(null, context.keyRecord)),
        DELETE_EXACT_ASYNC((view, context) -> view.deleteExactAsync(null, context.fullRecord)),
        GET_AND_DELETE_ASYNC((view, context) -> view.getAndDeleteAsync(null, context.keyRecord)),
        DELETE_ALL_ASYNC((view, context) -> view.deleteAllAsync(null, List.of(context.keyRecord))),
        DELETE_ALL_EXACT_ASYNC((view, context) -> view.deleteAllExactAsync(null, List.of(context.keyRecord))),
        @SuppressWarnings({"rawtypes", "unchecked"})
        STREAM_DATA((view, context) -> {
            CompletableFuture<?> future;
            try (var publisher = new SimplePublisher()) {
                future = view.streamData(publisher, null);
                publisher.submit(context.fullRecord);
            }
            return future;
        }),
        QUERY_ASYNC((view, context) -> view.queryAsync(null, null)),
        QUERY_ASYNC_WITH_INDEX_NAME((view, context) -> view.queryAsync(null, null, null)),
        QUERY_ASYNC_WITH_INDEX_NAME_AND_OPTS((view, context) -> view.queryAsync(null, null, null, null));

        private final BiFunction<RecordView<Object>, RecordContext<Object>, CompletableFuture<?>> action;

        RecordViewAsyncOperation(BiFunction<RecordView<Object>, RecordContext<Object>, CompletableFuture<?>> action) {
            this.action = action;
        }

        <R> CompletableFuture<?> executeOn(RecordView<R> tableView, RecordContext<R> context) {
            return action.apply((RecordView<Object>) tableView, (RecordContext<Object>) context);
        }
    }

    private static class RecordContext<R> {
        final R keyRecord;
        final R fullRecord;
        final R anotherFullRecord;

        private RecordContext(R keyRecord, R fullRecord, R anotherFullRecord) {
            this.keyRecord = keyRecord;
            this.fullRecord = fullRecord;
            this.anotherFullRecord = anotherFullRecord;
        }
    }

    @SuppressWarnings("rawtypes")
    private enum RecordViewKind {
        PLAIN, BINARY;

        RecordView view() {
            return this == PLAIN ? plainRecordView() : binaryRecordView();
        }

        RecordContext context() {
            return this == PLAIN ? plainRecordContext() : binaryRecordContext();
        }
    }
}
