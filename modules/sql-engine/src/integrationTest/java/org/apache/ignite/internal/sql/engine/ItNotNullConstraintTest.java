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

package org.apache.ignite.internal.sql.engine;

import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrows;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.SubmissionPublisher;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.apache.ignite.lang.MarshallerException;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.DataStreamerItem;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * NOT NULL constraint tests.
 */
public abstract class ItNotNullConstraintTest extends ClusterPerClassIntegrationTest {

    @AfterEach
    public void clear() {
        dropAllTables();
    }

    protected abstract Ignite ignite();

    protected final Table table(String name) {
        return ignite().tables().table(name);
    }

    protected void runSql(String stmt) {
        Ignite ignite = ignite();
        try (ResultSet<SqlRow> rs = ignite.sql().execute(null, stmt)) {
            assertNotNull(rs);
        }
    }

    @Test
    public void testSqlNotNullConstraints() {
        sql("CREATE TABLE t1 (id INTEGER PRIMARY KEY, int_col INTEGER NOT NULL)");

        // INSERT
        assertThrowsSqlException(
                Sql.CONSTRAINT_VIOLATION_ERR,
                "Column 'ID' does not allow NULLs",
                () -> runSql("INSERT INTO t1 VALUES(NULL, 1)"));

        // KV case
        assertThrowsSqlException(
                Sql.CONSTRAINT_VIOLATION_ERR,
                "Column 'INT_COL' does not allow NULLs",
                () -> runSql("INSERT INTO t1 VALUES(1, NULL)"));

        // General case

        assertThrowsSqlException(
                Sql.CONSTRAINT_VIOLATION_ERR,
                "Column 'INT_COL' does not allow NULLs",
                () -> runSql("INSERT INTO t1 VALUES(1, (SELECT NULL))"));

        assertThrowsSqlException(
                Sql.CONSTRAINT_VIOLATION_ERR,
                "Column 'INT_COL' does not allow NULLs",
                () -> runSql("INSERT INTO t1 SELECT 1, NULL"));

        // UPDATE
        sql("INSERT INTO t1 VALUES(1, 42)");

        assertThrowsSqlException(
                Sql.STMT_VALIDATION_ERR,
                "Cannot update field \"ID\". Primary key columns are not modifiable",
                () -> runSql("UPDATE t1 SET id = NULL WHERE val = 42"));

        // KV case
        assertThrowsSqlException(
                Sql.CONSTRAINT_VIOLATION_ERR,
                "Column 'INT_COL' does not allow NULLs",
                () -> runSql("UPDATE t1 SET int_col = null WHERE id = 1"));

        // General case

        assertThrowsSqlException(
                Sql.CONSTRAINT_VIOLATION_ERR,
                "Column 'INT_COL' does not allow NULLs",
                () -> runSql("UPDATE t1 SET int_col = null"));

        // MERGE
        runSql("CREATE TABLE t2 (id INTEGER PRIMARY KEY, int_col INTEGER NOT NULL)");

        runSql("INSERT INTO t2 VALUES (1, 42)");

        assertThrowsSqlException(
                Sql.CONSTRAINT_VIOLATION_ERR,
                "Column 'INT_COL' does not allow NULLs",
                () -> runSql("MERGE INTO t2 dst USING t1 src ON dst.id = src.id WHEN MATCHED THEN UPDATE SET int_col = NULL"));

        runSql("INSERT INTO t1 VALUES (2, 71)");

        assertThrowsSqlException(
                Sql.CONSTRAINT_VIOLATION_ERR,
                "Column 'INT_COL' does not allow NULLs",
                () -> runSql("MERGE INTO t2 dst USING t1 src ON dst.id = src.id "
                        + "WHEN NOT MATCHED THEN INSERT (id, int_col) VALUES (src.id, NULL)"));

    }

    @Test
    public void testKeyValueView() {
        sql("CREATE TABLE kv (id INTEGER PRIMARY KEY, val INTEGER NOT NULL)");

        Table table = table("KV");

        {
            KeyValueView<Integer, Integer> view = table.keyValueView(Integer.class, Integer.class);
            assertThrows(MarshallerException.class, () -> {
                view.put(null, 1, null);
            }, "Column 'VAL' does not allow NULLs");
        }

        {
            KeyValueView<Tuple, Tuple> view = table.keyValueView();
            assertThrows(MarshallerException.class, () -> {
                view.put(null, Tuple.create(Map.of("id", 1)), Tuple.create());
            }, "Column 'VAL' does not allow NULLs");
        }

        {
            KeyValueView<Integer, Val> view = table.keyValueView(Integer.class, Val.class);
            assertThrows(MarshallerException.class, () -> {
                Val val = new Val();
                view.put(null, 1, val);
            }, "Column 'VAL' does not allow NULLs");
        }
    }

    @Test
    public void testRecordView() {
        sql("CREATE TABLE kv (id INTEGER PRIMARY KEY, val INTEGER NOT NULL)");

        Table table = table("KV");

        {
            RecordView<Rec> view = table.recordView(Rec.class);

            assertThrows(MarshallerException.class, () -> {
                Rec rec = new Rec();
                view.insert(null, rec);
            }, "Column 'ID' does not allow NULLs");
        }

        {
            RecordView<Rec> view = table.recordView(Rec.class);
            assertThrows(MarshallerException.class, () -> {
                Rec rec = new Rec();
                rec.id = 42;
                view.insert(null, rec);
            }, "Column 'VAL' does not allow NULLs");
        }

        {
            RecordView<Tuple> view = table.recordView();
            assertThrows(MarshallerException.class, () -> {
                view.insert(null, Tuple.create(Map.of("id", 1)));
            }, "Column 'VAL' does not allow NULLs");
        }
    }

    @Test
    public void testKeyValueViewDataStreamer() {
        sql("CREATE TABLE kv (id INTEGER PRIMARY KEY, val INTEGER NOT NULL)");

        Table table = table("KV");

        {
            KeyValueView<Tuple, Tuple> view = table.keyValueView();

            checkDataStreamer(view, new SimpleEntry<>(Tuple.create(Map.of("id", 1)), Tuple.create()), "Column 'VAL' does not allow NULLs");
        }

        {
            KeyValueView<Integer, Integer> view = table.keyValueView(Integer.class, Integer.class);
            checkDataStreamer(view, new SimpleEntry<>(1, null), "Column 'VAL' does not allow NULLs");
        }

        {
            KeyValueView<Integer, Val> view = table.keyValueView(Integer.class, Val.class);
            Val val = new Val();
            checkDataStreamer(view, new SimpleEntry<>(1, val), "Column 'VAL' does not allow NULLs");
        }
    }

    @Test
    public void testRecordViewDataStreamer() {
        sql("CREATE TABLE kv (id INTEGER PRIMARY KEY, val INTEGER NOT NULL)");

        Table table = table("KV");

        {
            RecordView<Tuple> view = table.recordView();
            checkDataStreamer(view, Tuple.create(Map.of("id", 1)), "Column 'VAL' does not allow NULLs");
        }

        {
            RecordView<Rec> view = table.recordView(Rec.class);
            Rec rec = new Rec();
            checkDataStreamer(view, rec, "Column 'ID' does not allow NULLs");
        }

        {
            RecordView<Rec> view = table.recordView(Rec.class);
            Rec rec = new Rec();
            rec.id = 1;
            checkDataStreamer(view, rec, "Column 'VAL' does not allow NULLs");
        }
    }

    private static <K, V> void checkDataStreamer(KeyValueView<K, V> view, Entry<K, V> item, String error) {
        CompletableFuture<Void> streamerFut;

        try (var publisher = new SubmissionPublisher<DataStreamerItem<Entry<K, V>>>()) {
            streamerFut = view.streamData(publisher, null);
            publisher.submit(DataStreamerItem.of(item));
        }

        assertThrows(MarshallerException.class, () -> await(streamerFut), error);
    }

    private static <R> void checkDataStreamer(RecordView<R> view, R item, String error) {
        CompletableFuture<Void> streamerFut;

        try (var publisher = new SubmissionPublisher<DataStreamerItem<R>>()) {
            streamerFut = view.streamData(publisher, null);
            publisher.submit(DataStreamerItem.of(item));
        }

        assertThrows(MarshallerException.class, () -> await(streamerFut), error);
    }

    static class Val {
        @SuppressWarnings("unused")
        Integer val;
    }

    static class Rec {
        Integer id;
        @SuppressWarnings("unused")
        Integer val;
    }
}
