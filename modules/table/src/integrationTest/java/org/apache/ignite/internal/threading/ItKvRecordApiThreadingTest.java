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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.PublicApiThreadingTests.anIgniteThread;
import static org.apache.ignite.internal.PublicApiThreadingTests.asyncContinuationPool;
import static org.apache.ignite.internal.TestWrappers.unwrapTableManager;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.PublicApiThreadingTests;
import org.apache.ignite.internal.streamer.SimplePublisher;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.lang.AsyncCursor;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.criteria.CriteriaQueryOptions;
import org.apache.ignite.table.criteria.CriteriaQuerySource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Enum;

class ItKvRecordApiThreadingTest extends ClusterPerClassIntegrationTest {
    private static final String TABLE_NAME = "test";

    private static final int KEY = 1;

    private static final Record KEY_RECORD = new Record(1, "");

    // Setting a minimum page size to ensure that CriteriaQuerySource#query() returns
    // a non-closed cursor even after we call its second page.
    private static final CriteriaQueryOptions criteriaOptions = CriteriaQueryOptions.builder().pageSize(1).build();

    @Override
    protected int initialNodes() {
        return 1;
    }

    @BeforeAll
    void createTable() {
        sql("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, val VARCHAR)");

        plainKeyValueView().putAll(null, Map.of(KEY + 1, "two", KEY + 2, "three"));
    }

    private static KeyValueView<Integer, String> plainKeyValueView() {
        return testTable().keyValueView(Integer.class, String.class);
    }

    private static KeyValueView<Tuple, Tuple> binaryKeyValueView() {
        return testTable().keyValueView();
    }

    private static Table testTable() {
        return CLUSTER.aliveNode().tables().table(TABLE_NAME);
    }

    private static Table testTableForInternalUse() {
        TableManager internalIgniteTables = unwrapTableManager(CLUSTER.aliveNode().tables());
        return internalIgniteTables.table(TABLE_NAME);
    }

    private static KeyValueView<Integer, String> plainKeyValueViewForInternalUse() {
        return testTableForInternalUse().keyValueView(Integer.class, String.class);
    }

    private static KeyValueView<Tuple, Tuple> binaryKeyValueViewForInternalUse() {
        return testTableForInternalUse().keyValueView();
    }

    @BeforeEach
    void upsertRecord() {
        KeyValueView<Integer, String> view = plainKeyValueView();

        // #KEY is used by tests related to KV operations and queries.
        view.put(null, KEY, "one");
    }

    @CartesianTest
    void keyValueViewFuturesCompleteInContinuationsPool(
            @Enum KeyValueViewAsyncOperation operation,
            @Enum KeyValueViewKind kind
    ) {
        assumeTrue(
                kind.supportsGetNullable() || !operation.isGetNullable(),
                "Skipping the test as getNullable() is not supported by views of kind " + kind
        );

        KeyValueView<?, ?> tableView = kind.view();

        CompletableFuture<Thread> completerFuture = forcingSwitchFromUserThread(
                () -> operation.executeOn(tableView, kind.context())
                        .thenApply(unused -> Thread.currentThread())
        );

        assertThat(completerFuture, willBe(asyncContinuationPool()));
    }

    private static <T> T forcingSwitchFromUserThread(Supplier<? extends T> action) {
        return PublicApiThreadingTests.tryToSwitchFromUserThreadWithDelayedSchemaSync(CLUSTER.aliveNode(), action);
    }

    private static KeyValueContext<Integer, String> plainKeyValueContext() {
        return new KeyValueContext<>(KEY, "one", "two");
    }

    private static KeyValueContext<Tuple, Tuple> binaryKeyValueContext() {
        return new KeyValueContext<>(Tuple.create().set("id", KEY), Tuple.create().set("val", "one"), Tuple.create().set("val", "two"));
    }

    private static RecordView<Record> plainRecordViewForInternalUse() {
        return testTableForInternalUse().recordView(Record.class);
    }

    @CartesianTest
    void recordViewFuturesCompleteInContinuationsPool(
            @Enum RecordViewAsyncOperation operation,
            @Enum RecordViewKind kind
    ) {
        RecordView<?> tableView = kind.view();

        CompletableFuture<Thread> completerFuture = forcingSwitchFromUserThread(
                () -> operation.executeOn(tableView, kind.context())
                        .thenApply(unused -> Thread.currentThread())
        );

        assertThat(completerFuture, willBe(asyncContinuationPool()));
    }

    private static RecordView<Tuple> binaryRecordViewForInternalUse() {
        return testTableForInternalUse().recordView();
    }

    private static RecordView<Record> plainRecordView() {
        return testTable().recordView(Record.class);
    }

    private static RecordView<Tuple> binaryRecordView() {
        return testTable().recordView();
    }

    @CartesianTest
    void keyValueViewFuturesFromInternalCallsAreNotResubmittedToContinuationsPool(
            @Enum KeyValueViewAsyncOperation operation,
            @Enum KeyValueViewKind kind
    ) {
        assumeTrue(kind.supportsGetNullable() || !operation.isGetNullable());

        KeyValueView<?, ?> tableView = kind.viewForInternalUse();

        CompletableFuture<Thread> completerFuture = forcingSwitchFromUserThread(
                () -> operation.executeOn(tableView, kind.context())
                        .thenApply(unused -> Thread.currentThread())
        );

        assertThat(completerFuture, willBe(anIgniteThread()));
    }

    @CartesianTest
    void recordViewFuturesFromInternalCallsAreNotResubmittedToContinuationsPool(
            @Enum RecordViewAsyncOperation operation,
            @Enum RecordViewKind kind
    ) {
        RecordView<?> tableView = kind.viewForInternalUse();

        CompletableFuture<Thread> completerFuture = forcingSwitchFromUserThread(
                () -> operation.executeOn(tableView, kind.context())
                        .thenApply(unused -> Thread.currentThread())
        );

        assertThat(completerFuture, willBe(anIgniteThread()));
    }

    private static RecordContext<Record> plainRecordContext() {
        return new RecordContext<>(KEY_RECORD, new Record(KEY, "one"), new Record(KEY, "two"));
    }

    private static RecordContext<Tuple> binaryRecordContext() {
        return new RecordContext<>(KEY_RECORD.toKeyTuple(), new Record(KEY, "one").toFullTuple(), new Record(KEY, "two").toFullTuple());
    }

    @CartesianTest
    void commonViewFuturesCompleteInContinuationsPool(@Enum CommonViewAsyncOperation operation, @Enum ViewKind kind) {
        CriteriaQuerySource<?> tableView = kind.criteriaQuerySource();

        CompletableFuture<Thread> completerFuture = forcingSwitchFromUserThread(
                () -> operation.executeOn(tableView)
                        .thenApply(unused -> Thread.currentThread())
        );

        assertThat(completerFuture, willBe(asyncContinuationPool()));
    }

    @CartesianTest
    void commonViewFuturesFromInternalCallsAreNotResubmittedToContinuationsPool(
            @Enum CommonViewAsyncOperation operation,
            @Enum ViewKind kind
    ) {
        CriteriaQuerySource<?> tableView = kind.criteriaQuerySourceForInternalUse();

        CompletableFuture<Thread> completerFuture = forcingSwitchFromUserThread(
                () -> operation.executeOn(tableView)
                        .thenApply(unused -> Thread.currentThread())
        );

        assertThat(completerFuture, willBe(anIgniteThread()));
    }

    @CartesianTest
    void asyncCursorFuturesCompleteInContinuationsPool(@Enum AsyncCursorAsyncOperation operation, @Enum ViewKind kind) throws Exception {
        AsyncCursor<?> firstPage = kind.criteriaQuerySource()
                .queryAsync(null, null, null, criteriaOptions).get(10, SECONDS);

        CompletableFuture<Thread> completerFuture = operation.executeOn(firstPage)
                        .thenApply(unused -> Thread.currentThread());

        // The future might get completed in the calling thread as we don't force a wait inside Ignite
        // (because we cannot do this with fetching next page or closing).
        assertThat(completerFuture, willBe(
                either(is(Thread.currentThread())).or(asyncContinuationPool())
        ));
    }

    @CartesianTest
    void asyncCursorFuturesFromInternalCallsAreNotResubmittedToContinuationsPool(
            @Enum AsyncCursorAsyncOperation operation,
            @Enum ViewKind kind
    ) throws Exception {
        AsyncCursor<?> firstPage = kind.criteriaQuerySourceForInternalUse()
                .queryAsync(null, null, null, criteriaOptions).get(10, SECONDS);

        CompletableFuture<Thread> completerFuture = operation.executeOn(firstPage)
                .thenApply(unused -> Thread.currentThread());

        // The future might get completed in the calling thread as we don't force a wait inside Ignite
        // (because we cannot do this with fetching next page or closing).
        assertThat(completerFuture, willBe(
                either(is(Thread.currentThread())).or(anIgniteThread())
        ));
    }

    /**
     * This test differs from {@link #asyncCursorFuturesCompleteInContinuationsPool(AsyncCursorAsyncOperation, ViewKind)} in that it obtains
     * the future to test from a call on a cursor obtained from a cursor, not from a view.
     */
    @CartesianTest
    void asyncCursorFuturesAfterFetchCompleteInContinuationsPool(@Enum AsyncCursorAsyncOperation operation, @Enum ViewKind kind)
            throws Exception {
        AsyncCursor<?> firstPage = kind.criteriaQuerySource()
                .queryAsync(null, null, null, criteriaOptions).get(10, SECONDS);
        AsyncCursor<?> secondPage = firstPage.fetchNextPage().get(10, SECONDS);

        CompletableFuture<Thread> completerFuture = operation.executeOn(secondPage)
                .thenApply(unused -> Thread.currentThread());

        // The future might get completed in the calling thread as we don't force a wait inside Ignite
        // (because we cannot do this with fetching next page or closing).
        assertThat(completerFuture, willBe(
                either(is(Thread.currentThread())).or(asyncContinuationPool())
        ));
    }

    /**
     * This test differs from
     * {@link #asyncCursorFuturesFromInternalCallsAreNotResubmittedToContinuationsPool(AsyncCursorAsyncOperation, ViewKind)} in that
     * it obtains the future to test from a call on a cursor obtained from a cursor, not from a view.
     */
    @CartesianTest
    void asyncCursorFuturesAfterFetchFromInternalCallsAreNotResubmittedToContinuationsPool(
            @Enum AsyncCursorAsyncOperation operation,
            @Enum ViewKind kind
    ) throws Exception {
        AsyncCursor<?> firstPage = kind.criteriaQuerySourceForInternalUse()
                .queryAsync(null, null, null, criteriaOptions).get(10, SECONDS);
        AsyncCursor<?> secondPage = firstPage.fetchNextPage().get(10, SECONDS);

        CompletableFuture<Thread> completerFuture = operation.executeOn(secondPage)
                .thenApply(unused -> Thread.currentThread());

        // The future might get completed in the calling thread as we don't force a wait inside Ignite
        // (because we cannot do this with fetching next page or closing).
        assertThat(completerFuture, willBe(
                either(is(Thread.currentThread())).or(anIgniteThread())
        ));
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
        STREAM_DATA((view, context) -> {
            CompletableFuture<?> future;
            try (var publisher = new SimplePublisher<Entry<Object, Object>>()) {
                future = view.streamData(publisher, null);
                publisher.submit(Map.entry(context.key, context.usualValue));
            }
            return future;
        });

        private final BiFunction<KeyValueView<Object, Object>, KeyValueContext<Object, Object>, CompletableFuture<?>> action;

        KeyValueViewAsyncOperation(BiFunction<KeyValueView<Object, Object>, KeyValueContext<Object, Object>, CompletableFuture<?>> action) {
            this.action = action;
        }

        CompletableFuture<?> executeOn(KeyValueView<?, ?> tableView, KeyValueContext<?, ?> context) {
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

    private enum KeyValueViewKind {
        PLAIN, BINARY;

        KeyValueView<?, ?> view() {
            return this == PLAIN ? plainKeyValueView() : binaryKeyValueView();
        }

        KeyValueView<?, ?> viewForInternalUse() {
            return this == PLAIN ? plainKeyValueViewForInternalUse() : binaryKeyValueViewForInternalUse();
        }

        KeyValueContext<?, ?> context() {
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
        STREAM_DATA((view, context) -> {
            CompletableFuture<?> future;
            try (var publisher = new SimplePublisher<>()) {
                future = view.streamData(publisher, null);
                publisher.submit(context.fullRecord);
            }
            return future;
        });

        private final BiFunction<RecordView<Object>, RecordContext<Object>, CompletableFuture<?>> action;

        RecordViewAsyncOperation(BiFunction<RecordView<Object>, RecordContext<Object>, CompletableFuture<?>> action) {
            this.action = action;
        }

        CompletableFuture<?> executeOn(RecordView<?> tableView, RecordContext<?> context) {
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

    private enum RecordViewKind {
        PLAIN, BINARY;

        RecordView<?> view() {
            return this == PLAIN ? plainRecordView() : binaryRecordView();
        }

        RecordView<?> viewForInternalUse() {
            return this == PLAIN ? plainRecordViewForInternalUse() : binaryRecordViewForInternalUse();
        }

        RecordContext<?> context() {
            return this == PLAIN ? plainRecordContext() : binaryRecordContext();
        }
    }

    private enum CommonViewAsyncOperation {
        QUERY_ASYNC(view -> view.queryAsync(null, null)),
        QUERY_ASYNC_WITH_INDEX_NAME(view -> view.queryAsync(null, null, null)),
        QUERY_ASYNC_WITH_INDEX_NAME_AND_OPTS(view -> view.queryAsync(null, null, null, null));

        private final Function<CriteriaQuerySource<Object>, CompletableFuture<?>> action;

        CommonViewAsyncOperation(Function<CriteriaQuerySource<Object>, CompletableFuture<?>> action) {
            this.action = action;
        }

        CompletableFuture<?> executeOn(CriteriaQuerySource<?> tableView) {
            return action.apply((CriteriaQuerySource<Object>) tableView);
        }
    }

    private enum ViewKind {
        PLAIN_KEY_VALUE(() -> plainKeyValueView(), () -> plainKeyValueViewForInternalUse()),
        BINARY_KEY_VALUE(() -> binaryKeyValueView(), () -> binaryKeyValueViewForInternalUse()),
        PLAIN_RECORD(() -> plainRecordView(), () -> plainRecordViewForInternalUse()),
        BINARY_RECORD(() -> binaryRecordView(), () -> binaryRecordViewForInternalUse());

        private final Supplier<CriteriaQuerySource<?>> viewSupplier;
        private final Supplier<CriteriaQuerySource<?>> viewForInternalUseSupplier;

        ViewKind(Supplier<CriteriaQuerySource<?>> viewSupplier, Supplier<CriteriaQuerySource<?>> viewForInternalUseSupplier) {
            this.viewSupplier = viewSupplier;
            this.viewForInternalUseSupplier = viewForInternalUseSupplier;
        }

        CriteriaQuerySource<?> criteriaQuerySource() {
            return viewSupplier.get();
        }

        CriteriaQuerySource<?> criteriaQuerySourceForInternalUse() {
            return viewForInternalUseSupplier.get();
        }
    }

    private enum AsyncCursorAsyncOperation {
        FETCH_NEXT_PAGE(cursor -> cursor.fetchNextPage()),
        CLOSE(cursor -> cursor.closeAsync());

        private final Function<AsyncCursor<Object>, CompletableFuture<?>> action;

        AsyncCursorAsyncOperation(Function<AsyncCursor<Object>, CompletableFuture<?>> action) {
            this.action = action;
        }

        CompletableFuture<?> executeOn(AsyncCursor<?> cursor) {
            return action.apply((AsyncCursor<Object>) cursor);
        }
    }
}
