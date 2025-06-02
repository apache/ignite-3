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

package org.apache.ignite.client.compatibility;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ResultSetMetadata;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.QualifiedName;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Base class for client compatibility tests. Contains actual tests logic, without infrastructure initialization.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ClientCompatibilityTestBase {
    private static final String TABLE_NAME_TEST = "TEST";
    private static final String TABLE_NAME_ALL_COLUMNS = "ALL_COLUMNS";

    private final AtomicInteger idGen = new AtomicInteger(1000);

    IgniteClient client;

    @BeforeAll
    public void beforeAll() throws Exception {
        createDefaultTables();
    }

    @Test
    public void testClusterNodes() {
        Collection<ClusterNode> nodes = client.clusterNodes();
        assertThat(nodes, Matchers.hasSize(1));
        assertEquals("defaultNode", nodes.iterator().next().name());
    }

    @Test
    @Disabled("IGNITE-25514")
    public void testTableByName() {
        Table testTable = client.tables().table(TABLE_NAME_TEST);
        assertNotNull(testTable);

        assertEquals(TABLE_NAME_TEST, testTable.qualifiedName().objectName());
    }

    @Test
    @Disabled("IGNITE-25514")
    public void testTableByQualifiedName() {
        Table testTable = client.tables().table(QualifiedName.fromSimple(TABLE_NAME_TEST));
        assertNotNull(testTable);

        assertEquals(TABLE_NAME_TEST, testTable.qualifiedName().objectName());
    }

    @Test
    public void testTables() {
        List<Table> tables = client.tables().tables();

        List<String> tableNames = tables.stream()
                .map(t -> t.qualifiedName().objectName())
                .collect(Collectors.toList());

        assertThat(tableNames, Matchers.containsInAnyOrder(TABLE_NAME_TEST, TABLE_NAME_ALL_COLUMNS));
    }

    @Test
    public void testSqlColumnMeta() {
        try (var cursor = client.sql().execute(null, "select * from " + TABLE_NAME_ALL_COLUMNS)) {
            ResultSetMetadata meta = cursor.metadata();
            assertNotNull(meta);

            List<ColumnMetadata> cols = meta.columns();
            assertEquals(15, cols.size());
            assertThat(cols.get(0).toString(), containsString("name=ID, type=INT32, precision=10"));
            assertThat(cols.get(1).toString(), containsString("name=BYTE, type=INT8, precision=3"));
            assertThat(cols.get(2).toString(), containsString("name=SHORT, type=INT16, precision=5"));
            assertThat(cols.get(3).toString(), containsString("name=INT, type=INT32, precision=10"));
            assertThat(cols.get(4).toString(), containsString("name=LONG, type=INT64, precision=19"));
            assertThat(cols.get(5).toString(), containsString("name=FLOAT, type=FLOAT, precision=7"));
            assertThat(cols.get(6).toString(), containsString("name=DOUBLE, type=DOUBLE, precision=15"));
            assertThat(cols.get(7).toString(), containsString("name=DEC, type=DECIMAL, precision=32767, scale=0"));
            assertThat(cols.get(8).toString(), containsString("name=STRING, type=STRING, precision=65536"));
            assertThat(cols.get(9).toString(), containsString("name=UUID, type=UUID, precision=-1"));
            assertThat(cols.get(10).toString(), containsString("name=DT, type=DATE, precision=0"));
            assertThat(cols.get(11).toString(), containsString("name=TM, type=TIME, precision=0"));
            assertThat(cols.get(12).toString(), containsString("name=TS, type=DATETIME, precision=6"));
            assertThat(cols.get(13).toString(), containsString("name=BOOL, type=BOOLEAN, precision=1"));
            assertThat(cols.get(14).toString(), containsString("name=BYTES, type=BYTE_ARRAY, precision=65536"));
        }
    }

    @Test
    public void testRecordView() {
        assert false : "TODO";
    }

    @Test
    @Disabled("IGNITE-25545")
    public void testTxCommit() {
        int id = idGen.incrementAndGet();
        Tuple key = Tuple.create().set("id", id);

        RecordView<Tuple> view = table(TABLE_NAME_TEST).recordView();

        assertNull(view.get(null, key));

        client.transactions().runInTransaction(tx -> {
            Tuple tuple = Tuple.create().set("id", id).set("name", "testTxCommit");
            view.insert(tx, tuple);
        });

        Tuple res = view.get(null, key);
        assertNotNull(res);
        assertEquals("testTxCommit", res.stringValue("name"));
    }

    @Test
    @Disabled("IGNITE-25545")
    public void testTxRollback() {
        int id = idGen.incrementAndGet();
        Tuple key = Tuple.create().set("id", id);

        RecordView<Tuple> view = table(TABLE_NAME_TEST).recordView();

        assertNull(view.get(null, key));

        Transaction tx = client.transactions().begin();
        view.insert(tx, Tuple.create().set("id", id).set("name", "testTxRollback"));
        tx.rollback();

        assertNull(view.get(null, key));
    }

    @Test
    public void testTxReadOnly() {
        int id = idGen.incrementAndGet();
        Tuple key = Tuple.create().set("id", id);

        RecordView<Tuple> view = table(TABLE_NAME_TEST).recordView();
        Transaction tx = client.transactions().begin(new TransactionOptions().readOnly(true));

        view.insert(null, Tuple.create().set("id", id).set("name", "testTxReadOnly"));
        assertNull(view.get(tx, key), "Read-only transaction shows snapshot of data in the past.");

        tx.rollback();
    }

    @Test
    public void testCompute() {
        assert false : "TODO";
    }

    @Test
    public void testStreamer() {
        assert false : "TODO";
    }

    private void createDefaultTables() {
        if (ddl("CREATE TABLE IF NOT EXISTS " + TABLE_NAME_TEST + " (id INT PRIMARY KEY, name VARCHAR)")) {
            sql("INSERT INTO " + TABLE_NAME_TEST + " (id, name) VALUES (1, 'test')");
        }

        if (ddl("CREATE TABLE IF NOT EXISTS " + TABLE_NAME_ALL_COLUMNS + " (id INT PRIMARY KEY, byte TINYINT, short SMALLINT, " +
                "int INT, long BIGINT, float REAL, double DOUBLE, dec DECIMAL, " +
                "string VARCHAR, uuid UUID, dt DATE, tm TIME, ts TIMESTAMP, bool BOOLEAN, bytes VARBINARY)")) {
            sql("INSERT INTO " + TABLE_NAME_ALL_COLUMNS + " (id, byte, short, int, long, float, double, dec, " +
                    "string, uuid, dt, tm, ts, bool, bytes) VALUES " +
                    "(1, 1, 2, 3, 4, 5.0, 6.0, 7.0, 'test', '10000000-2000-3000-4000-500000000000'::UUID, " +
                    "date '2023-01-01', time '12:00:00', timestamp '2023-01-01 12:00:00', true, X'01020304')");
        }
    }

    private @Nullable List<SqlRow> sql(String sql) {
        try (var cursor = client.sql().execute(null, sql)) {
            if (cursor.hasRowSet()) {
                List<SqlRow> rows = new ArrayList<>();
                cursor.forEachRemaining(rows::add);
                return rows;
            } else {
                return null;
            }
        }
    }

    private boolean ddl(String sql) {
        try (var cursor = client.sql().execute(null, sql)) {
            return cursor.wasApplied();
        }
    }

    private Table table(String tableName) {
        // TODO IGNITE-25514 Use client.tables().table().
        return client.tables().tables().stream()
                .filter(t -> t.qualifiedName().objectName().equals(tableName))
                .findFirst()
                .orElseThrow();
    }
}
