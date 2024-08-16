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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import javax.annotation.Nullable;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.lang.MarshallerException;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end test check mappings for case sensitive/insensitive column names.
 */
public class ItColumnNameMappingTest extends ClusterPerClassIntegrationTest {
    @BeforeAll
    public void createTables() {
        sql("CREATE TABLE test_simple (id BIGINT PRIMARY KEY, ColumnName INT)");
        sql("CREATE TABLE test_quoted (id BIGINT PRIMARY KEY, \"ColumnName\" INT)");
    }

    @BeforeEach
    public void clearTables() {
        for (Table t : CLUSTER.aliveNode().tables().tables()) {
            sql("DELETE FROM " + t.name());
        }
    }

    @AfterAll
    public void dropTables() {
        for (Table t : CLUSTER.aliveNode().tables().tables()) {
            sql("DROP TABLE " + t.name());
        }
    }

    @Test
    public void accessQuotedColumnViaKvView() {
        Ignite node = CLUSTER.aliveNode();
        Table table = node.tables().table("test_quoted");

        KeyValueView<Tuple, Tuple> view = table.keyValueView();

        // Case-insensitive names
        assertThrows(MarshallerException.class, () ->
                view.put(null, Tuple.create().set("id", 1L), Tuple.create().set("columnname", 1)));
        assertThrows(MarshallerException.class, () ->
                view.put(null, Tuple.create().set("id", 2L), Tuple.create().set("COLUMNNAME", 2)));
        assertThrows(MarshallerException.class, () ->
                view.put(null, Tuple.create().set("id", 3L), Tuple.create().set("ColumnName", 3)));

        assertNull(view.get(null, Tuple.create().set("id", 1L)));
        assertNull(view.get(null, Tuple.create().set("id", 2L)));
        assertNull(view.get(null, Tuple.create().set("id", 3L)));

        // Valid case-sensitive name
        view.put(null, Tuple.create().set("id", 4L), Tuple.create().set("\"ColumnName\"", 4));
        checkQuotedColumn(4, view.get(null, Tuple.create().set("id", 4L)));

        // Invalid names
        assertThrows(MarshallerException.class, () ->
                view.put(null, Tuple.create().set("id", 5L), Tuple.create().set("\"columnname\"", 5)));
        assertThrows(MarshallerException.class, () ->
                view.put(null, Tuple.create().set("id", 6L), Tuple.create().set("\"COLUMNNAME\"", 6)));

        assertNull(view.get(null, Tuple.create().set("id", 5L)));
        assertNull(view.get(null, Tuple.create().set("id", 6L)));
    }

    @Test
    public void accessQuotedColumnViaRecordView() {
        Ignite node = CLUSTER.aliveNode();
        Table table = node.tables().table("test_quoted");

        RecordView<Tuple> view = table.recordView();

        // Case-insensitive names
        assertThrows(MarshallerException.class, () ->
                view.insert(null, Tuple.create().set("id", 1L).set("columnname", 1)));
        assertThrows(MarshallerException.class, () ->
                view.insert(null, Tuple.create().set("id", 2L).set("COLUMNNAME", 2)));
        assertThrows(MarshallerException.class, () ->
                view.insert(null, Tuple.create().set("id", 3L).set("ColumnName", 3)));

        assertNull(view.get(null, Tuple.create().set("id", 1L)));
        assertNull(view.get(null, Tuple.create().set("id", 2L)));
        assertNull(view.get(null, Tuple.create().set("id", 3L)));

        // Valid case-sensitive name
        view.insert(null, Tuple.create().set("id", 4L).set("\"ColumnName\"", 4));
        checkQuotedColumn(4, view.get(null, Tuple.create().set("id", 4L)));

        // Invalid names
        assertThrows(MarshallerException.class, () ->
                view.insert(null, Tuple.create().set("id", 5L).set("\"columnname\"", 5)));
        assertThrows(MarshallerException.class, () ->
                view.insert(null, Tuple.create().set("id", 6L).set("\"COLUMNNAME\"", 6)));

        assertNull(view.get(null, Tuple.create().set("id", 5L)));
        assertNull(view.get(null, Tuple.create().set("id", 6L)));
    }

    @Test
    public void accessUnquotedColumnViaKvView() {
        Ignite node = CLUSTER.aliveNode();
        Table table = node.tables().table("test_simple");

        KeyValueView<Tuple, Tuple> view = table.keyValueView();

        // Case-insensitive names
        view.put(null, Tuple.create().set("id", 1L), Tuple.create().set("columnname", 1));
        view.put(null, Tuple.create().set("id", 2L), Tuple.create().set("COLUMNNAME", 2));
        view.put(null, Tuple.create().set("id", 3L), Tuple.create().set("ColumnName", 3));

        checkUnquotedColumn(1, view.get(null, Tuple.create().set("id", 1L)));
        checkUnquotedColumn(2, view.get(null, Tuple.create().set("id", 2L)));
        checkUnquotedColumn(3, view.get(null, Tuple.create().set("id", 3L)));

        // Equivalent case-sensitive name
        view.put(null, Tuple.create().set("id", 4L), Tuple.create().set("\"COLUMNNAME\"", 4));
        checkUnquotedColumn(4, view.get(null, Tuple.create().set("id", 4L)));

        // Invalid names
        assertThrows(MarshallerException.class, () ->
                view.put(null, Tuple.create().set("id", 5L), Tuple.create().set("\"columnname\"", 5)));
        assertThrows(MarshallerException.class, () ->
                view.put(null, Tuple.create().set("id", 6L), Tuple.create().set("\"ColumnName\"", 6)));

        assertNull(view.get(null, Tuple.create().set("id", 5L)));
        assertNull(view.get(null, Tuple.create().set("id", 6L)));
    }

    @Test
    public void accessUnquotedColumnViaRecordView() {
        Ignite node = CLUSTER.aliveNode();
        Table table = node.tables().table("test_simple");

        RecordView<Tuple> view = table.recordView();

        // Case-insensitive names
        view.insert(null, Tuple.create().set("id", 1L).set("columnname", 1));
        view.insert(null, Tuple.create().set("id", 2L).set("COLUMNNAME", 2));
        view.insert(null, Tuple.create().set("id", 3L).set("ColumnName", 3));

        checkUnquotedColumn(1, view.get(null, Tuple.create().set("id", 1L)));
        checkUnquotedColumn(2, view.get(null, Tuple.create().set("id", 2L)));
        checkUnquotedColumn(3, view.get(null, Tuple.create().set("id", 3L)));

        // Equivalent case-sensitive name
        view.insert(null, Tuple.create().set("id", 4L).set("\"COLUMNNAME\"", 4));
        checkUnquotedColumn(4, view.get(null, Tuple.create().set("id", 4L)));

        // Invalid names
        assertThrows(MarshallerException.class, () ->
                view.insert(null, Tuple.create().set("id", 5L).set("\"columnname\"", 5)));
        assertThrows(MarshallerException.class, () ->
                view.insert(null, Tuple.create().set("id", 6L).set("\"ColumnName\"", 6)));

        assertNull(view.get(null, Tuple.create().set("id", 5L)));
        assertNull(view.get(null, Tuple.create().set("id", 6L)));
    }

    private static void checkQuotedColumn(@Nullable Object expectedValue, @Nullable Tuple tuple) {
        assertNotNull(tuple);

        assertNull(tuple.valueOrDefault("columnname", null));
        assertNull(tuple.valueOrDefault("COLUMNNAME", null));
        assertNull(tuple.valueOrDefault("ColumnName", null));

        assertNull(tuple.valueOrDefault("\"columnname\"", null));
        assertNull(tuple.valueOrDefault("\"COLUMNNAME\"", null));
        assertEquals(expectedValue, tuple.valueOrDefault("\"ColumnName\"", null));
    }

    private static void checkUnquotedColumn(@Nullable Object expectedValue, @Nullable Tuple tuple) {
        assertNotNull(tuple);

        assertEquals(expectedValue, tuple.valueOrDefault("columnname", null));
        assertEquals(expectedValue, tuple.valueOrDefault("COLUMNNAME", null));
        assertEquals(expectedValue, tuple.valueOrDefault("ColumnName", null));
        ;

        assertNull(tuple.valueOrDefault("\"columnname\"", null));
        assertEquals(expectedValue, tuple.valueOrDefault("\"COLUMNNAME\"", null));
        assertNull(tuple.valueOrDefault("\"ColumnName\"", null));
    }
}
