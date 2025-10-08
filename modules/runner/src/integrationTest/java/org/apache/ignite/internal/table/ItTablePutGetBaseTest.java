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

package org.apache.ignite.internal.table;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Objects;
import java.util.stream.Stream;
import org.apache.ignite.catalog.annotations.Column;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.lang.Cursor;
import org.apache.ignite.lang.MarshallerException;
import org.apache.ignite.lang.NullableValue;
import org.apache.ignite.table.IgniteTables;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Base tests to check that rows can be inserted.
 */
public abstract class ItTablePutGetBaseTest extends ClusterPerClassIntegrationTest {

    abstract IgniteTables tables();

    @AfterEach
    void dropTables() {
        dropAllTables();
    }

    @ParameterizedTest
    @MethodSource("tableDefinitions")
    @SuppressWarnings("DataFlowIssue")
    void putGetInTableWithDifferentOrdersOfColumnsInKeyAndColocationKey(String tableDefinitionDdl) {
        sql(tableDefinitionDdl);

        {
            KeyValueView<Tuple, Tuple> kvView = tables().table("test").keyValueView();

            for (int i = 0; i < 10; i++) {
                Tuple key = Tuple.create()
                        .set("c1", i)
                        .set("c3", i * 100);

                Tuple val = Tuple.create()
                        .set("c2", i * 10);

                kvView.put(null, key, val);
            }

            for (int i = 0; i < 10; i++) {
                Tuple key = Tuple.create()
                        .set("c1", i)
                        .set("c3", i * 100);

                assertEquals(i * 10, kvView.get(null, key).intValue("c2"));
                assertEquals(i * 10, kvView.get(null, key).intValue(0));
            }
        }

        {
            RecordView<Tuple> recordView = tables().table("test").recordView();

            for (int i = 100; i < 110; i++) {
                Tuple record = Tuple.create()
                        .set("c1", i)
                        .set("c2", i * 10)
                        .set("c3", i * 100);

                recordView.insert(null, record);
            }

            for (int i = 100; i < 110; i++) {
                Tuple key = Tuple.create()
                        .set("c1", i)
                        .set("c3", i * 100);

                assertEquals(i * 10, recordView.get(null, key).intValue("c2"));
                assertEquals(i * 10, recordView.get(null, key).intValue(1));
            }
        }
    }

    @Test
    public void testSingleColumnTableKeyValueView() {
        String tableName = "TEST_TABLE_1";

        sql("CREATE TABLE " + tableName + " (id int primary key)");
        sql("INSERT INTO " + tableName + " (id) VALUES (1)");

        // Tuples.
        KeyValueView<Tuple, Tuple> kvTupleView = tables().table(tableName).keyValueView();
        Tuple key = Tuple.create().set("id", 1);
        Tuple val = Tuple.create();

        kvTupleView.put(null, key, val);
        Tuple res = kvTupleView.get(null, key);

        assertEquals(val, res);

        // Classes.
        KeyValueView<Integer, Void> kvPrimitiveView = tables().table(tableName).keyValueView(Integer.class, Void.class);
        int key2 = 2;

        kvPrimitiveView.put(null, key2, null);
        NullableValue<Void> res2 = kvPrimitiveView.getNullable(null, key2);

        assertNull(res2.get());
    }

    @Test
    public void testCaseSensitiveMapping() {
        sql("CREATE TABLE sampleTable (\"lowercase\" varchar(255), id int primary key)");

        RecordView<PojoExample> sampleTable = tables().table("sampleTable").recordView(PojoExample.class);

        PojoExample pojoExample = new PojoExample();
        pojoExample.id = 1;
        pojoExample.lowercase = "test";

        sampleTable.upsert(null, pojoExample);

        try (Cursor<PojoExample> cursor = sampleTable.query(null, null)) {
            assertThat(cursor.hasNext(), is(true));
            assertThat(cursor.next(), equalTo(pojoExample));
        }

        RecordView<PojoExampleIncorrect> sampleTableIncorrect = tables().table("sampleTable").recordView(PojoExampleIncorrect.class);

        PojoExampleIncorrect pojoExampleIncorrect = new PojoExampleIncorrect();
        pojoExampleIncorrect.id = 1;
        pojoExampleIncorrect.lowercase = "test";

        assertThrows(MarshallerException.class, () -> sampleTableIncorrect.upsert(null, pojoExampleIncorrect));
    }

    static class PojoExampleIncorrect {
        int id;

        String lowercase;
    }

    static class PojoExample {
        int id;

        @Column("\"lowercase\"")
        String lowercase;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof PojoExample)) {
                return false;
            }
            PojoExample that = (PojoExample) o;
            return id == that.id && Objects.equals(lowercase, that.lowercase);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, lowercase);
        }

        @Override
        public String toString() {
            return S.toString(this);
        }
    }

    private static Stream<Arguments> tableDefinitions() {
        return Stream.of(
                "CREATE TABLE test (c1 INT, c2 INT, c3 INT, PRIMARY KEY (c1, c3)) COLOCATE BY (c1)",
                "CREATE TABLE test (c1 INT, c2 INT, c3 INT, PRIMARY KEY (c1, c3)) COLOCATE BY (c3)",
                "CREATE TABLE test (c1 INT, c2 INT, c3 INT, PRIMARY KEY (c1, c3)) COLOCATE BY (c1, c3)",
                "CREATE TABLE test (c1 INT, c2 INT, c3 INT, PRIMARY KEY (c1, c3)) COLOCATE BY (c3, c1)",
                "CREATE TABLE test (c1 INT, c2 INT, c3 INT, PRIMARY KEY (c3, c1)) COLOCATE BY (c1)",
                "CREATE TABLE test (c1 INT, c2 INT, c3 INT, PRIMARY KEY (c3, c1)) COLOCATE BY (c3)",
                "CREATE TABLE test (c1 INT, c2 INT, c3 INT, PRIMARY KEY (c3, c1)) COLOCATE BY (c1, c3)",
                "CREATE TABLE test (c1 INT, c2 INT, c3 INT, PRIMARY KEY (c3, c1)) COLOCATE BY (c3, c1)"
        ).map(Arguments::of);
    }
}
