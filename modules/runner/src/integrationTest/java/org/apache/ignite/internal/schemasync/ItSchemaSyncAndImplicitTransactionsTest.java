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

package org.apache.ignite.internal.schemasync;

import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.executeUpdate;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests about Schema Sync interaction with implicit transactions.
 */
@SuppressWarnings("resource")
class ItSchemaSyncAndImplicitTransactionsTest extends ClusterPerClassIntegrationTest {
    private static final int NODES_TO_START = 1;

    private static final String TABLE_NAME = "test";

    private static final int ITERATIONS = 100;

    private static final int BATCH_SIZE = 10;

    @Override
    protected int initialNodes() {
        return NODES_TO_START;
    }

    @BeforeEach
    void createTable() {
        executeUpdate(
                "CREATE TABLE " + TABLE_NAME + " (id int, val varchar NOT NULL, PRIMARY KEY USING HASH (id))",
                CLUSTER.aliveNode().sql()
        );
    }

    @AfterEach
    void dropTable() {
        executeUpdate("DROP TABLE IF EXISTS " + TABLE_NAME, CLUSTER.aliveNode().sql());
    }

    private static void makeCompatibleChangeTo(String tableName) {
        executeUpdate("ALTER TABLE " + tableName + " ALTER COLUMN val DROP NOT NULL", CLUSTER.aliveNode().sql());
    }

    @ParameterizedTest
    @EnumSource(BinaryRecordViewOperation.class)
    @DisplayName("Concurrent schema changes are transparent for implicit transactions via binary record views")
    public void schemaChangesTransparencyForBinaryRecordView(BinaryRecordViewOperation operation) {
        RecordView<Tuple> view = CLUSTER.aliveNode().tables().table(TABLE_NAME).recordView();

        CompletableFuture<Void> operationsFuture = runAsync(() -> {
            for (int i = 0; i < ITERATIONS; i++) {
                operation.execute(i, view);
            }
        });

        makeCompatibleChangeTo(TABLE_NAME);

        assertThat(operationsFuture, willCompleteSuccessfully());
    }

    @ParameterizedTest
    @EnumSource(PlainRecordViewOperation.class)
    @DisplayName("Concurrent schema changes are transparent for implicit transactions via plain record views")
    public void schemaChangesTransparencyForPlainRecordView(PlainRecordViewOperation operation) {
        RecordView<Record> view = CLUSTER.aliveNode().tables().table(TABLE_NAME).recordView(Record.class);

        CompletableFuture<Void> operationsFuture = runAsync(() -> {
            for (int i = 0; i < ITERATIONS; i++) {
                operation.execute(i, view);
            }
        });

        makeCompatibleChangeTo(TABLE_NAME);

        assertThat(operationsFuture, willCompleteSuccessfully());
    }

    @ParameterizedTest
    @EnumSource(BinaryKvViewOperation.class)
    @DisplayName("Concurrent schema changes are transparent for implicit transactions via binary KV views")
    public void schemaChangesTransparencyForBinaryKvView(BinaryKvViewOperation operation) {
        KeyValueView<Tuple, Tuple> view = CLUSTER.aliveNode().tables().table(TABLE_NAME).keyValueView();

        CompletableFuture<Void> operationsFuture = runAsync(() -> {
            for (int i = 0; i < ITERATIONS; i++) {
                operation.execute(i, view);
            }
        });

        makeCompatibleChangeTo(TABLE_NAME);

        assertThat(operationsFuture, willCompleteSuccessfully());
    }

    @ParameterizedTest
    @EnumSource(PlainKvViewOperation.class)
    @DisplayName("Concurrent schema changes are transparent for implicit transactions via plain KV views")
    public void schemaChangesTransparencyForPlainKvView(PlainKvViewOperation operation) {
        KeyValueView<Integer, String> view = CLUSTER.aliveNode().tables().table(TABLE_NAME).keyValueView(Integer.class, String.class);

        CompletableFuture<Void> operationsFuture = runAsync(() -> {
            for (int i = 0; i < ITERATIONS; i++) {
                operation.execute(i, view);
            }
        });

        makeCompatibleChangeTo(TABLE_NAME);

        assertThat(operationsFuture, willCompleteSuccessfully());
    }

    private static Tuple keyTuple(int id) {
        return Tuple.create().set("id", id);
    }

    private static List<Tuple> keyTuples(int base) {
        return IntStream.range(base, base + BATCH_SIZE)
                .mapToObj(ItSchemaSyncAndImplicitTransactionsTest::keyTuple)
                .collect(toList());
    }

    private enum BinaryRecordViewOperation {
        SINGLE_READ((view, base) -> view.get(null, keyTuple(base))),
        MULTI_PARTITION_READ((view, base) -> view.getAll(null, keyTuples(base))),
        SINGLE_WRITE((view, base) -> view.upsert(null, fullTuple(base))),
        MULTI_PARTITION_WRITE((view, base) -> view.upsertAll(null, fullTuples(base)));

        private final BiConsumer<RecordView<Tuple>, Integer> action;

        BinaryRecordViewOperation(BiConsumer<RecordView<Tuple>, Integer> action) {
            this.action = action;
        }

        void execute(int base, RecordView<Tuple> view) {
            action.accept(view, base);
        }

        private static Tuple fullTuple(int id) {
            return Tuple.create().set("id", id).set("val", Integer.toString(id));
        }

        private static List<Tuple> fullTuples(int base) {
            return IntStream.range(base, base + BATCH_SIZE)
                    .mapToObj(BinaryRecordViewOperation::fullTuple)
                    .collect(toList());
        }
    }

    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private static class Record {
        private int id;
        private @Nullable String val;

        private Record() {
        }

        private Record(int id, @Nullable String val) {
            this.id = id;
            this.val = val;
        }
    }

    private enum PlainRecordViewOperation {
        SINGLE_READ((view, base) -> view.get(null, keyRecord(base))),
        MULTI_PARTITION_READ((view, base) -> view.getAll(null, keyRecords(base))),
        SINGLE_WRITE((view, base) -> view.upsert(null, fullRecord(base))),
        MULTI_PARTITION_WRITE((view, base) -> view.upsertAll(null, fullRecords(base)));

        private final BiConsumer<RecordView<Record>, Integer> action;

        PlainRecordViewOperation(BiConsumer<RecordView<Record>, Integer> action) {
            this.action = action;
        }

        void execute(int base, RecordView<Record> view) {
            action.accept(view, base);
        }

        private static Record keyRecord(int id) {
            return new Record(id, null);
        }

        private static List<Record> keyRecords(int base) {
            return IntStream.range(base, base + BATCH_SIZE)
                    .mapToObj(PlainRecordViewOperation::keyRecord)
                    .collect(toList());
        }

        private static Record fullRecord(int id) {
            return new Record(id, Integer.toString(id));
        }

        private static List<Record> fullRecords(int base) {
            return IntStream.range(base, base + BATCH_SIZE)
                    .mapToObj(PlainRecordViewOperation::fullRecord)
                    .collect(toList());
        }
    }

    private enum BinaryKvViewOperation {
        SINGLE_READ((view, base) -> view.get(null, keyTuple(base))),
        MULTI_PARTITION_READ((view, base) -> view.getAll(null, keyTuples(base))),
        SINGLE_WRITE((view, base) -> view.put(null, keyTuple(base), valueTuple(base))),
        MULTI_PARTITION_WRITE((view, base) -> view.putAll(null, mapOfTuples(base)));

        private final BiConsumer<KeyValueView<Tuple, Tuple>, Integer> action;

        BinaryKvViewOperation(BiConsumer<KeyValueView<Tuple, Tuple>, Integer> action) {
            this.action = action;
        }

        void execute(int base, KeyValueView<Tuple, Tuple> view) {
            action.accept(view, base);
        }

        private static Tuple valueTuple(int id) {
            return Tuple.create().set("val", Integer.toString(id));
        }

        private static Map<Tuple, Tuple> mapOfTuples(int base) {
            return IntStream.range(base, base + BATCH_SIZE)
                    .boxed()
                    .collect(toMap(ItSchemaSyncAndImplicitTransactionsTest::keyTuple, BinaryKvViewOperation::valueTuple));
        }
    }

    private enum PlainKvViewOperation {
        SINGLE_READ((view, base) -> view.get(null, base)),
        MULTI_PARTITION_READ((view, base) -> view.getAll(null, keys(base))),
        SINGLE_WRITE((view, base) -> view.put(null, base, Integer.toString(base))),
        MULTI_PARTITION_WRITE((view, base) -> view.putAll(null, mapToValues(base)));

        private final BiConsumer<KeyValueView<Integer, String>, Integer> action;

        PlainKvViewOperation(BiConsumer<KeyValueView<Integer, String>, Integer> action) {
            this.action = action;
        }

        void execute(int base, KeyValueView<Integer, String> view) {
            action.accept(view, base);
        }

        private static List<Integer> keys(int base) {
            return IntStream.range(base, base + BATCH_SIZE)
                    .boxed()
                    .collect(toList());
        }

        private static Map<Integer, String> mapToValues(int base) {
            return IntStream.range(base, base + BATCH_SIZE)
                    .boxed()
                    .collect(toMap(identity(), n -> Integer.toString(n)));
        }
    }
}
