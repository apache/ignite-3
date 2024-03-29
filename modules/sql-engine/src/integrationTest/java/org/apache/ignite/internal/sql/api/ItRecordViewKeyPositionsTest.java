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

package org.apache.ignite.internal.sql.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.LocalDate;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.sql.BaseSqlIntegrationTest;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.manager.IgniteTables;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests for record views with different key column placement.
 */
public class ItRecordViewKeyPositionsTest extends BaseSqlIntegrationTest {

    private static final AtomicInteger ID_NUM = new AtomicInteger();

    @BeforeAll
    public void setup() {
        sql("CREATE TABLE key_val (intCol INT, boolCol VARCHAR, dateCol DATE, strCol VARCHAR, PRIMARY KEY (intCol, strCol))");
        sql("CREATE TABLE key_val_flip (intCol INT, boolCol VARCHAR, dateCol DATE, strCol VARCHAR, PRIMARY KEY (strCol, intCol))");

        sql("CREATE TABLE val_key (boolCol VARCHAR, intCol INT, dateCol DATE, strCol VARCHAR, PRIMARY KEY (intCol, strCol))");
        sql("CREATE TABLE val_key_flip (boolCol VARCHAR, intCol INT, dateCol DATE, strCol VARCHAR, PRIMARY KEY (strCol, intCol))");
    }

    @Override
    protected int initialNodes() {
        return 1;
    }

    @ParameterizedTest
    @MethodSource("recordViews")
    public void testInsertGet(RecordViewSource<Object> view) {
        Object record1 = view.newRecord();
        view.view.insert(null, record1);

        Object retrieved = view.view.get(null, view.toKey(record1));
        assertEquals(record1, view.toValue(retrieved));
    }

    @ParameterizedTest
    @MethodSource("recordViews")
    public void testGetAll(RecordViewSource<Object> view) {
        Object record1 = view.newRecord();
        view.view.insert(null, record1);

        Object record2 = view.newRecord();
        view.view.insert(null, record2);

        List<Object> records = view.view.getAll(null, List.of(view.toKey(record1), view.toKey(record2)));
        List<Object> values = records.stream()
                .map(view::toValue)
                .collect(Collectors.toList());

        assertEquals(List.of(record1, record2), values);
    }

    @SuppressWarnings("FieldMayBeFinal")
    private static class Pojo {
        private int key1;
        @Nullable
        private String key2;
        @Nullable
        private String val1;
        private long val2;

        @SuppressWarnings("unused") // Required by serializer.
        Pojo() {
            this(0, null, null, 0L);
        }

        Pojo(int key1, String key2) {
            this(key1, key2, null, 0L);
        }

        Pojo(int key1, @Nullable String key2, @Nullable String val1, long val2) {
            this.key1 = key1;
            this.key2 = key2;
            this.val1 = val1;
            this.val2 = val2;
        }
    }

    @Test
    public void dddd() {
        String query = "CREATE TABLE X"
                + " (val1 VARCHAR, key1 INT, val2 BIGINT, key2 VARCHAR, PRIMARY KEY (key1, key2)) ";

        IgniteImpl ignite = CLUSTER.aliveNode();

        ResultSet<SqlRow> rs = ignite.sql().execute(null, query);
        rs.close();
        Table table = ignite.tables().table("X");

        var recView = table.recordView(Pojo.class);

        Pojo key = new Pojo(1, "key2");
        Pojo key2 = new Pojo(2, "key3");

        Pojo val = new Pojo(1, "key2", "val1", 2L);
        recView.insert(null, val);

        // removeAll.
        Collection<Pojo> removeAllRes = recView.deleteAll(null, List.of(key, key2));
        assertEquals(1, removeAllRes.size());
        assertEquals(key2.key1, removeAllRes.iterator().next().key1);
    }

    @ParameterizedTest
    @MethodSource("recordViews")
    public void testDeleteAll(RecordViewSource<Object> view) {
        Object record1 = view.newRecord();
        view.view.insert(null, record1);

        Object record2 = view.newRecord();
        view.view.insert(null, record2);

        List<Object> keysToDelete = List.of(view.toKey(record1), view.toKey(record2));
        List<Object> notDeleted = view.view.deleteAll(null, keysToDelete);
        assertEquals(List.of(), notDeleted);

        assertNull(view.view.get(null, view.toKey(record1)));
        assertNull(view.view.get(null, view.toKey(record2)));
    }

    @ParameterizedTest
    @MethodSource("recordViews")
    public void testDeleteAllExact(RecordViewSource<Object> view) {
        Object record1 = view.newRecord();
        view.view.insert(null, record1);

        Object record2 = view.newRecord();
        view.view.insert(null, record2);

        List<Object> recordsToDelete = List.of(record1, record2);
        List<Object> notDeleted = view.view.deleteAllExact(null, recordsToDelete);

        assertEquals(List.of(), notDeleted);

        assertNull(view.view.get(null, view.toKey(record1)));
        assertNull(view.view.get(null, view.toKey(record2)));
    }

    abstract static class RecordViewSource<R> {

        private final RecordView<R> view;

        RecordViewSource(RecordView<R> view) {
            this.view = view;
        }

        abstract R newRecord();

        abstract R toKey(R record);

        abstract R toValue(R record);
    }

    static class Record {
        @IgniteToStringInclude
        int intCol;
        @IgniteToStringInclude
        String boolCol;
        @IgniteToStringInclude
        String strCol;
        @IgniteToStringInclude
        LocalDate dateCol;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Record rec = (Record) o;
            return intCol == rec.intCol && Objects.equals(boolCol, rec.boolCol) && Objects.equals(strCol, rec.strCol) && Objects.equals(
                    dateCol, rec.dateCol);
        }

        @Override
        public int hashCode() {
            return Objects.hash(intCol, boolCol, strCol, dateCol);
        }

        @Override
        public String toString() {
            return S.toString(this);
        }
    }

    static class RecordViewPojo extends RecordViewSource<Record> {

        RecordViewPojo(IgniteTables tables, String tableName) {
            super(tables.table(tableName).recordView(Mapper.of(Record.class)));
        }

        @Override
        Record newRecord() {
            int intCol = ID_NUM.incrementAndGet();

            Record record = new Record();
            record.intCol = intCol;
            record.boolCol = String.valueOf(record.intCol*2);
            record.strCol = String.valueOf(record.intCol);
            record.dateCol = LocalDate.now();
            return record;
        }

        @Override
        Record toKey(Record record) {
            return record;
        }

        @Override
        Record toValue(Record record) {
            return record;
        }
    }

    static class RecordViewBinary extends RecordViewSource<Tuple> {

        RecordViewBinary(IgniteTables tables, String tableName) {
            super(tables.table(tableName).recordView());
        }

        @Override
        Tuple newRecord() {
            int intCol = ID_NUM.incrementAndGet();;
            Tuple record = Tuple.create();
            record.set("intCol", intCol);
            record.set("boolCol", String.valueOf(intCol*2));
            record.set("strCol", String.valueOf(intCol));
            record.set("dateCol", LocalDate.now());
            return record;
        }

        @Override
        Tuple toKey(Tuple tuple) {
            Tuple record = Tuple.create();
            record.set("intCol", tuple.value("intCol"));
            record.set("strCol", tuple.value("strCol"));
            return record;
        }

        @Override
        Tuple toValue(Tuple record) {
            return Tuple.create(record);
        }
    }

    private List<Arguments> recordViews() {
        IgniteTables tables = CLUSTER.aliveNode().tables();

        return List.of(
                Arguments.of(Named.named("server key_val_key", new RecordViewPojo(tables, "KEY_VAL"))),
                Arguments.of(Named.named("server key_val_key_flip", new RecordViewPojo(tables, "KEY_VAL_FLIP"))),
                Arguments.of(Named.named("server val_key_key", new RecordViewPojo(tables, "VAL_KEY"))),
                Arguments.of(Named.named("server val_key_key_flip", new RecordViewPojo(tables, "VAL_KEY_FLIP"))),

                Arguments.of(Named.named("server bin key_val_key", new RecordViewBinary(tables, "KEY_VAL"))),
                Arguments.of(Named.named("server bin key_val_key_flip", new RecordViewBinary(tables, "KEY_VAL_FLIP"))),
                Arguments.of(Named.named("server bin val_key_key", new RecordViewBinary(tables, "VAL_KEY"))),
                Arguments.of(Named.named("server bin val_key_key_flip", new RecordViewBinary(tables, "VAL_KEY_FLIP")))
        );
    }
}
