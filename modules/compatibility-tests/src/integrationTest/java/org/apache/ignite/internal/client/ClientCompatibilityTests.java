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

package org.apache.ignite.internal.client;

import static org.apache.ignite.internal.CompatibilityTestCommon.TABLE_NAME_ALL_COLUMNS;
import static org.apache.ignite.internal.CompatibilityTestCommon.TABLE_NAME_TEST;
import static org.apache.ignite.internal.jobs.Jobs.JOBS_UNIT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.SubmissionPublisher;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.BroadcastJobTarget;
import org.apache.ignite.compute.ComputeException;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.internal.CompatibilityTestCommon;
import org.apache.ignite.internal.jobs.DeploymentUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.sql.BatchedArguments;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.Statement;
import org.apache.ignite.table.DataStreamerItem;
import org.apache.ignite.table.DataStreamerOptions;
import org.apache.ignite.table.DataStreamerReceiverDescriptor;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Client compatibility tests. Interface to allow "multiple inheritance" of test methods.
 */
@SuppressWarnings({"resource", "DataFlowIssue"})
public interface ClientCompatibilityTests {
    IgniteClient client();

    AtomicInteger idGen();

    default String tableNamePrefix() {
        return "";
    }

    @Test
    default void testClusterNodes() {
        Collection<ClusterNode> nodes = client().cluster().nodes();
        assertThat(nodes, Matchers.hasSize(1));
    }

    @SuppressWarnings("deprecation")
    @Test
    default void testClusterNodesDeprecated() {
        Collection<ClusterNode> nodes = client().clusterNodes();
        assertThat(nodes, Matchers.hasSize(1));
    }

    @Test
    default void testTableByName() {
        Table testTable = client().tables().table(TABLE_NAME_TEST);
        assertNotNull(testTable);

        assertEquals(tableNamePrefix() + TABLE_NAME_TEST, testTable.name());
    }

    @Test
    default void testTableByQualifiedName() {
        Table testTable = client().tables().table(QualifiedName.fromSimple(TABLE_NAME_TEST));
        assertNotNull(testTable);

        assertEquals(TABLE_NAME_TEST, testTable.qualifiedName().objectName());
    }

    @Test
    default void testTables() {
        List<Table> tables = client().tables().tables();

        List<String> tableNames = tables.stream()
                .map(Table::name)
                .collect(Collectors.toList());

        assertThat(
                tableNames,
                Matchers.containsInAnyOrder(tableNamePrefix() + TABLE_NAME_TEST, tableNamePrefix() + TABLE_NAME_ALL_COLUMNS));
    }

    @Test
    default void testSqlColumnMeta() {
        try (var cursor = client().sql().execute(null, "select * from " + TABLE_NAME_ALL_COLUMNS)) {
            ResultSetMetadata meta = cursor.metadata();
            assertNotNull(meta);

            List<ColumnMetadata> cols = meta.columns();
            assertEquals(16, cols.size());
            assertThat(cols.get(0).toString(), containsString("name=ID, type=INT32, precision=10"));
            assertThat(cols.get(1).toString(), containsString("name=BYTE, type=INT8, precision=3"));
            assertThat(cols.get(2).toString(), containsString("name=SHORT, type=INT16, precision=5"));
            assertThat(cols.get(3).toString(), containsString("name=INT, type=INT32, precision=10"));
            assertThat(cols.get(4).toString(), containsString("name=LONG, type=INT64, precision=19"));
            assertThat(cols.get(5).toString(), containsString("name=FLOAT, type=FLOAT, precision=7"));
            assertThat(cols.get(6).toString(), containsString("name=DOUBLE, type=DOUBLE, precision=15"));
            assertThat(cols.get(7).toString(), containsString("name=DEC, type=DECIMAL, precision=10, scale=1"));
            assertThat(cols.get(8).toString(), containsString("name=STRING, type=STRING, precision=65536"));
            assertThat(cols.get(9).toString(), containsString("name=GUID, type=UUID, precision=-1"));
            assertThat(cols.get(10).toString(), containsString("name=DT, type=DATE, precision=0"));
            assertThat(cols.get(11).toString(), containsString("name=TM, type=TIME, precision=9"));
            assertThat(cols.get(12).toString(), containsString("name=TS, type=DATETIME, precision=9"));
            assertThat(cols.get(13).toString(), containsString("name=TSTZ, type=TIMESTAMP, precision=6"));
            assertThat(cols.get(14).toString(), containsString("name=BOOL, type=BOOLEAN, precision=1"));
            assertThat(cols.get(15).toString(), containsString("name=BYTES, type=BYTE_ARRAY, precision=65536"));
        }
    }

    @Test
    default void testSqlSelectAllColumnTypes() {
        List<SqlRow> rows = sql("select * from " + TABLE_NAME_ALL_COLUMNS + " where id = 1");
        assertNotNull(rows);
        assertEquals(1, rows.size());

        SqlRow row = rows.get(0);
        assertEquals(1, row.intValue("id"));
        assertEquals((byte) 1, row.byteValue("byte"));
        assertEquals((short) 2, row.shortValue("short"));
        assertEquals(3, row.intValue("int"));
        assertEquals(4L, row.longValue("long"));
        assertEquals(5.0f, row.floatValue("float"));
        assertEquals(6.0d, row.doubleValue("double"));
        assertEquals(new BigDecimal("7"), row.decimalValue("dec"));
        assertEquals("test", row.stringValue("string"));
        assertEquals(UUID.fromString("10000000-2000-3000-4000-500000000000"), row.uuidValue("guid"));
        assertEquals(LocalDate.of(2023, 1, 1), row.dateValue("dt"));
        assertEquals(LocalTime.of(12, 0, 0), row.timeValue("tm"));
        assertEquals(LocalDateTime.of(2023, 1, 1, 12, 0, 0), row.datetimeValue("ts"));
        assertEquals(Instant.ofEpochSecond(1714946523), row.timestampValue("tstz"));
        assertTrue(row.booleanValue("bool"));
        assertArrayEquals(new byte[]{1, 2, 3, 4}, row.value("bytes"));
    }

    @Test
    default void testSqlMultiplePages() {
        int count = 12345;
        int minId = idGen().incrementAndGet();

        List<Tuple> tuples = IntStream.range(0, count)
                .map(x -> idGen().incrementAndGet())
                .mapToObj(id -> Tuple.create().set("id", id).set("name", "test" + id))
                .collect(Collectors.toList());

        RecordView<Tuple> view = table(TABLE_NAME_TEST).recordView();
        view.upsertAll(null, tuples);

        Statement statement = client().sql().statementBuilder()
                .query("SELECT * FROM " + TABLE_NAME_TEST + " WHERE id > ?")
                .pageSize(10)
                .build();

        try (var cursor = client().sql().execute(null, statement, minId)) {
            AtomicInteger rowCnt = new AtomicInteger();
            cursor.forEachRemaining(x -> rowCnt.incrementAndGet());

            assertEquals(count, rowCnt.get());
        }
    }

    @Test
    default void testSqlScript() {
        client().sql().executeScript("CREATE TABLE testSqlScript (id INT PRIMARY KEY, name VARCHAR)");
        assertEquals(1, sql("SELECT * FROM SYSTEM.TABLES WHERE NAME = 'TESTSQLSCRIPT'").size());

        client().sql().executeScript("DROP TABLE testSqlScript");
        assertEquals(0, sql("SELECT * FROM SYSTEM.TABLES WHERE NAME = 'TESTSQLSCRIPT'").size());
    }

    @Test
    default void testSqlBatch() {
        int id1 = idGen().incrementAndGet();
        int id2 = idGen().incrementAndGet();

        BatchedArguments args = BatchedArguments.create()
                .add(id1, "test1")
                .add(id2, "test2");

        client().sql().executeBatch(null, "INSERT INTO " + TABLE_NAME_TEST + " (id, name) VALUES (?, ?)", args);

        List<SqlRow> rows = sql("SELECT * FROM " + TABLE_NAME_TEST + " WHERE id IN (?, ?)", id1, id2);
        assertEquals(2, rows.size());
    }

    @Test
    default void testRecordViewOperations() {
        int id = idGen().incrementAndGet();
        int id2 = idGen().incrementAndGet();
        Tuple key = Tuple.create().set("id", id);
        Tuple key2 = Tuple.create().set("id", id2);

        RecordView<Tuple> view = table(TABLE_NAME_TEST).recordView();

        // Insert.
        assertTrue(view.insert(null, Tuple.create().set("id", id).set("name", "v1")));
        assertEquals("v1", view.get(null, key).stringValue("name"));
        assertFalse(view.insert(null, Tuple.create().set("id", id).set("name", "v2")));

        // Insert All.
        List<Tuple> insertAllRes = view.insertAll(
                null,
                List.of(Tuple.create().set("id", id).set("name", "v3"), Tuple.create().set("id", id2).set("name", "v4")));

        assertEquals(1, insertAllRes.size());
        assertEquals(id, insertAllRes.get(0).intValue(0));

        // Upsert.
        view.upsert(null, Tuple.create().set("id", id).set("name", "v2"));
        assertEquals("v2", view.get(null, key).stringValue("name"));

        // Get and upsert.
        Tuple oldValue = view.getAndUpsert(null, Tuple.create().set("id", id).set("name", "v5"));
        assertEquals("v2", oldValue.stringValue("name"));

        // Upsert All.
        view.upsertAll(null, List.of(Tuple.create().set("id", id).set("name", "v5"), Tuple.create().set("id", id2).set("name", "v6")));
        assertEquals("v5", view.get(null, key).stringValue("name"));
        assertEquals("v6", view.get(null, key2).stringValue("name"));

        // Contains.
        assertTrue(view.contains(null, key));
        assertFalse(view.contains(null, Tuple.create().set("id", -id)));

        // Contains all.
        assertTrue(view.containsAll(null, List.of(key, key2)));
        assertFalse(view.containsAll(null, List.of(key, Tuple.create().set("id", -id))));

        // Get.
        assertNotNull(view.get(null, key));
        assertNull(view.get(null, Tuple.create().set("id", -id)));

        // Get all.
        List<Tuple> keys = List.of(key, Tuple.create().set("id", -id));
        List<Tuple> results = view.getAll(null, keys);
        assertEquals(2, results.size());
        assertEquals("v5", results.get(0).stringValue("name"));
        assertNull(results.get(1));

        // Replace.
        assertTrue(view.replace(null, Tuple.create().set("id", id).set("name", "v7")));
        assertFalse(view.replace(null, Tuple.create().set("id", -id).set("name", "v8")));
        assertEquals("v7", view.get(null, key).stringValue("name"));

        // Replace exact.
        assertFalse(view.replace(null, Tuple.create().set("id", id).set("name", "-v7"), Tuple.create().set("id", id).set("name", "v8")));
        assertTrue(view.replace(null, Tuple.create().set("id", id).set("name", "v7"), Tuple.create().set("id", id).set("name", "v8")));
        assertEquals("v8", view.get(null, key).stringValue("name"));

        // Get and replace.
        Tuple old = view.getAndReplace(null, Tuple.create().set("id", id).set("name", "v9"));
        assertEquals("v8", old.stringValue("name"));
        assertEquals("v9", view.get(null, key).stringValue("name"));

        // Delete.
        assertTrue(view.delete(null, key));
        assertFalse(view.delete(null, key));
        assertNull(view.get(null, key));

        // Delete exact.
        assertFalse(view.deleteExact(null, Tuple.create().set("id", id2).set("name", "v9")));
        assertTrue(view.deleteExact(null, Tuple.create().set("id", id2).set("name", "v6")));

        // Get and delete.
        view.upsert(null, Tuple.create().set("id", id).set("name", "v10"));
        assertNull(view.getAndDelete(null, Tuple.create().set("id", -id)));

        Tuple getAndDelete = view.getAndDelete(null, Tuple.create().set("id", id));
        assertEquals("v10", getAndDelete.stringValue("name"));

        // Delete all.
        view.upsert(null, Tuple.create().set("id", id).set("name", "v11"));
        List<Tuple> deleteAllRes = view.deleteAll(null, List.of(Tuple.create().set("id", id), Tuple.create().set("id", id2)));

        assertEquals(1, deleteAllRes.size());
        assertEquals(id2, deleteAllRes.get(0).intValue(0));

        // Delete all exact.
        view.upsert(null, Tuple.create().set("id", id).set("name", "v12"));
        view.upsert(null, Tuple.create().set("id", id2).set("name", "v13"));

        List<Tuple> deleteAllExactRes = view.deleteAllExact(
                null, List.of(Tuple.create().set("id", id), Tuple.create().set("id", id2).set("name", "v13")));

        assertEquals(1, deleteAllExactRes.size());
        assertEquals(id, deleteAllExactRes.get(0).intValue(0));
    }

    @Test
    default void testKvViewOperations() {
        int id = idGen().incrementAndGet();
        int id2 = idGen().incrementAndGet();
        Tuple key = Tuple.create().set("id", id);
        Tuple key2 = Tuple.create().set("id", id2);

        KeyValueView<Tuple, Tuple> view = table(TABLE_NAME_TEST).keyValueView();

        // Insert.
        assertTrue(view.putIfAbsent(null, key, Tuple.create().set("name", "v1")));
        assertEquals("v1", view.get(null, key).stringValue("name"));

        assertFalse(view.putIfAbsent(null, key, Tuple.create().set("name", "v2")));

        // Insert All - not supported by KeyValueView.

        // Upsert.
        view.put(null, key, Tuple.create().set("name", "v2"));
        assertEquals("v2", view.get(null, key).stringValue("name"));

        // Get and upsert.
        Tuple oldValue = view.getAndPut(null, key, Tuple.create().set("name", "v3"));
        assertEquals("v2", oldValue.stringValue("name"));

        // Upsert all.
        view.putAll(null, Map.of(
                key, Tuple.create().set("name", "v4"),
                key2, Tuple.create().set("name", "v5")
        ));

        assertEquals("v4", view.get(null, key).stringValue("name"));
        assertEquals("v5", view.get(null, key2).stringValue("name"));

        // Contains.
        assertTrue(view.contains(null, key));
        assertFalse(view.contains(null, Tuple.create().set("id", -id)));

        // Contains all.
        assertTrue(view.containsAll(null, List.of(key, key2)));
        assertFalse(view.containsAll(null, List.of(key, Tuple.create().set("id", -id))));

        // Get.
        assertNotNull(view.get(null, key));
        assertNull(view.get(null, Tuple.create().set("id", -id)));

        // Get all.
        Map<Tuple, Tuple> getAllRes = view.getAll(null, List.of(key, key2, Tuple.create().set("id", -id)));
        assertEquals(2, getAllRes.size());
        assertEquals("v4", getAllRes.get(key).stringValue("name"));
        assertEquals("v5", getAllRes.get(key2).stringValue("name"));

        // Replace.
        assertTrue(view.replace(null, key, Tuple.create().set("name", "v6")));
        assertFalse(view.replace(null, Tuple.create().set("id", -id), Tuple.create().set("name", "v7")));
        assertEquals("v6", view.get(null, key).stringValue("name"));

        // Replace exact.
        assertFalse(view.replace(null, key, Tuple.create().set("name", "-v6"), Tuple.create().set("name", "v7")));
        assertTrue(view.replace(null, key, Tuple.create().set("name", "v6"), Tuple.create().set("name", "v7")));
        assertEquals("v7", view.get(null, key).stringValue("name"));

        // Get and replace.
        Tuple old = view.getAndReplace(null, key, Tuple.create().set("name", "v8"));
        assertEquals("v7", old.stringValue("name"));
        assertEquals("v8", view.get(null, key).stringValue("name"));

        // Delete.
        assertTrue(view.remove(null, key));
        assertFalse(view.remove(null, key));
        assertNull(view.get(null, key));

        // Delete exact.
        assertFalse(view.remove(null, key2, Tuple.create().set("name", "-v5")));
        assertTrue(view.remove(null, key2, Tuple.create().set("name", "v5")));
        assertNull(view.get(null, key2));

        // Get and delete.
        view.put(null, key, Tuple.create().set("name", "v9"));
        assertNull(view.getAndRemove(null, Tuple.create().set("id", -id)));
        Tuple getAndDelete = view.getAndRemove(null, key);
        assertEquals("v9", getAndDelete.stringValue("name"));
        assertNull(view.get(null, key));

        // Delete all.
        view.put(null, key, Tuple.create().set("name", "v10"));
        Collection<Tuple> deleteAllRes = view.removeAll(null, List.of(key, key2));

        assertEquals(1, deleteAllRes.size());
        assertEquals(key2, deleteAllRes.iterator().next());

        assertNull(view.get(null, key));
        assertNull(view.get(null, key2));
    }

    @Test
    default void testRecordViewAllColumnTypes() {
        RecordView<Tuple> view = table(TABLE_NAME_ALL_COLUMNS).recordView();

        int id = idGen().incrementAndGet();

        Tuple tuple = Tuple.create()
                .set("id", id)
                .set("byte", (byte) 1)
                .set("short", (short) 2)
                .set("int", 3)
                .set("long", 4L)
                .set("float", 5.5f)
                .set("double", 6.6d)
                .set("dec", new BigDecimal("7.7"))
                .set("string", "test")
                .set("guid", UUID.randomUUID())
                .set("dt", LocalDate.now())
                .set("tm", LocalTime.now())
                .set("ts", LocalDateTime.now())
                .set("tstz", Instant.ofEpochSecond(123456))
                .set("bool", true)
                .set("bytes", new byte[]{1, 2, 3, 4});

        assertTrue(view.insert(null, tuple));

        Tuple res = view.get(null, Tuple.create().set("id", id));
        assertNotNull(res);

        assertEquals(tuple, res);
    }

    @Test
    default void testTxCommit() {
        int id = idGen().incrementAndGet();
        Tuple key = Tuple.create().set("id", id);

        RecordView<Tuple> view = table(TABLE_NAME_TEST).recordView();

        assertNull(view.get(null, key));

        client().transactions().runInTransaction(tx -> {
            Tuple tuple = Tuple.create().set("id", id).set("name", "testTxCommit");
            view.insert(tx, tuple);
        });

        Tuple res = view.get(null, key);
        assertNotNull(res);
        assertEquals("testTxCommit", res.stringValue("name"));
    }

    @Test
    default void testTxRollback() {
        int id = idGen().incrementAndGet();
        Tuple key = Tuple.create().set("id", id);

        RecordView<Tuple> view = table(TABLE_NAME_TEST).recordView();

        assertNull(view.get(null, key));

        Transaction tx = client().transactions().begin();
        view.insert(tx, Tuple.create().set("id", id).set("name", "testTxRollback"));
        tx.rollback();

        assertNull(view.get(null, key));
    }

    @Test
    default void testTxReadOnly() {
        int id = idGen().incrementAndGet();
        Tuple key = Tuple.create().set("id", id);

        RecordView<Tuple> view = table(TABLE_NAME_TEST).recordView();

        // Start and activate a read-only transaction.
        Transaction tx = client().transactions().begin(new TransactionOptions().readOnly(true));
        assertNull(view.get(tx, key)); // Activate lazy tx.

        // Insert a record with an implicit tx.
        view.insert(null, Tuple.create().set("id", id).set("name", "testTxReadOnly"));

        // RO transaction should not see the changes made outside of it.
        assertNull(view.get(tx, key), "Read-only transaction shows snapshot of data in the past.");

        tx.rollback();
    }

    @Test
    default void testComputeMissingJob() {
        JobTarget target = JobTarget.anyNode(clusterNodes());
        JobDescriptor<Object, Object> desc = JobDescriptor.builder("test").build();

        var ex = assertThrows(ComputeException.class, () ->  client().compute().execute(target, desc, null));
        assertThat(ex.getMessage(), containsString("Cannot load job class by name 'test'"));
    }

    @ParameterizedTest
    @MethodSource("jobArgs")
    default void testComputeArgs(Object arg) {
        JobTarget target = JobTarget.anyNode(clusterNodes());
        JobDescriptor<Object, Object> desc = echoJobDescriptor();

        Object jobRes = client().compute().execute(target, desc, arg);

        if (arg instanceof byte[]) {
            assertArrayEquals((byte[]) arg, (byte[]) jobRes);
        } else {
            assertEquals(arg, jobRes);
        }
    }

    @Test
    default void testComputeExecute() {
        JobTarget target = JobTarget.anyNode(clusterNodes());
        JobDescriptor<Object, Object> desc = echoJobDescriptor();

        Object jobRes = client().compute().execute(target, desc, "test");
        assertEquals("test", jobRes);
    }

    @Test
    default void testComputeExecuteColocated() {
        JobTarget target = JobTarget.colocated(TABLE_NAME_TEST, Tuple.create().set("id", 1));
        JobDescriptor<Object, Object> desc = echoJobDescriptor();

        Object jobRes = client().compute().execute(target, desc, "test");
        assertEquals("test", jobRes);
    }

    @Test
    default void testComputeExecuteBroadcast() {
        BroadcastJobTarget target = BroadcastJobTarget.nodes(clusterNodes());
        JobDescriptor<Object, Object> desc = echoJobDescriptor();

        Collection<Object> jobRes = client().compute().execute(target, desc, "test");
        assertEquals("test", jobRes.iterator().next());
    }

    @Test
    default void testComputeExecuteBroadcastTable() {
        BroadcastJobTarget target = BroadcastJobTarget.table(TABLE_NAME_TEST);
        JobDescriptor<Object, Object> desc = echoJobDescriptor();

        Collection<Object> jobRes = client().compute().execute(target, desc, "test");
        assertEquals("test", jobRes.iterator().next());
    }

    @Test
    default void testStreamer() {
        RecordView<Tuple> view = table(TABLE_NAME_TEST).recordView();

        CompletableFuture<Void> streamFut;
        List<Tuple> keys = new ArrayList<>();

        try (var publisher = new SubmissionPublisher<DataStreamerItem<Tuple>>()) {
            streamFut = view.streamData(publisher, DataStreamerOptions.builder().pageSize(5).build());

            for (int i = 0; i < 100; i++) {
                Tuple item = Tuple.create().set("id", idGen().incrementAndGet()).set("name", "test" + i);
                publisher.submit(DataStreamerItem.of(item));

                keys.add(item);
            }
        }

        streamFut.join();

        List<Tuple> results = view.getAll(null, keys);
        assertEquals(keys.size(), results.size());
    }

    @Test
    default void testStreamerWithReceiver() {
        RecordView<Tuple> view = table(TABLE_NAME_TEST).recordView();

        CompletableFuture<Void> streamFut;

        DataStreamerReceiverDescriptor<Integer, Object, Integer> desc = DataStreamerReceiverDescriptor
                .<Integer, Object, Integer>builder("org.apache.ignite.internal.compute.EchoReceiver")
                .units(JOBS_UNIT)
                .build();

        var subscriber = new TestSubscriber<Integer>();
        List<Integer> expected = new ArrayList<>();

        try (var publisher = new SubmissionPublisher<Integer>()) {
            streamFut = view.streamData(
                    publisher,
                    desc,
                    x -> Tuple.create().set("id", x),
                    Function.identity(),
                    null,
                    subscriber,
                    DataStreamerOptions.builder().pageSize(3).build());

            for (int i = 0; i < 10; i++) {
                publisher.submit(i);
                expected.add(i);
            }
        }

        streamFut.join();

        List<Integer> sortedResults = subscriber.items.stream()
                .sorted()
                .collect(Collectors.toList());

        assertEquals(expected, sortedResults);
    }

    @Test
    default void testStreamerWithReceiverArg() {
        RecordView<Tuple> view = table(TABLE_NAME_TEST).recordView();

        CompletableFuture<Void> streamFut;

        DataStreamerReceiverDescriptor<Integer, String, String> desc = DataStreamerReceiverDescriptor
                .<Integer, String, String>builder("org.apache.ignite.internal.compute.EchoReceiver")
                .units(JOBS_UNIT)
                .build();

        var subscriber = new TestSubscriber<String>();

        try (var publisher = new SubmissionPublisher<Integer>()) {
            streamFut = view.streamData(
                    publisher,
                    desc,
                    x -> Tuple.create().set("id", x),
                    Function.identity(),
                    "arg",
                    subscriber,
                    DataStreamerOptions.builder().pageSize(2).build());

            for (int i = 0; i < 10; i++) {
                publisher.submit(i);
            }
        }

        streamFut.join();

        assertEquals("arg", subscriber.items.iterator().next());
    }

    /**
     * Initialize test data in the given Ignite instance.
     */
    default void initTestData(Ignite ignite) {
        CompatibilityTestCommon.createDefaultTables(ignite);
        DeploymentUtils.deployJobs();
    }

    default void close() {
        // No-op by default.
    }

    default Collection<ClusterNode> clusterNodes() {
        return client().cluster().nodes();
    }

    private @Nullable List<SqlRow> sql(String sql, Object... arguments) {
        return CompatibilityTestCommon.sql(client(), sql, arguments);
    }

    private Table table(String tableName) {
        return client().tables().table(tableName);
    }

    /**
     * Arguments for job execution tests.
     *
     * @return Array of arguments.
     */
    static Object[] jobArgs() {
        return new Object[]{
                true,
                (byte) 1,
                (short) 2,
                3,
                4L,
                5.5f,
                6.6d,
                new BigDecimal("7.7"),
                LocalDate.now(),
                LocalTime.now(),
                LocalDateTime.now(),
                Instant.ofEpochSecond(123456),
                UUID.randomUUID(),
                "test",
                new byte[]{1, 2, 3, 4},
                null
        };
    }

    private static JobDescriptor<Object, Object> echoJobDescriptor() {
        return JobDescriptor
                .builder("org.apache.ignite.internal.compute.Echo")
                .units(JOBS_UNIT)
                .build();
    }

    /**
     * Test subscriber.
     */
    class TestSubscriber<T> implements Subscriber<T> {
        List<T> items = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void onSubscribe(Subscription subscription) {
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(T item) {
            items.add(item);
        }

        @Override
        public void onError(Throwable throwable) {
        }

        @Override
        public void onComplete() {
        }
    }
}
