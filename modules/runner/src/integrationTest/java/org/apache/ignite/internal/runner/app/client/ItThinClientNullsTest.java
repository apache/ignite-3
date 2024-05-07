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

package org.apache.ignite.internal.runner.app.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.lang.NullableValue;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

/**
 * Thin client nulls in operations test.
 */
public class ItThinClientNullsTest extends ItAbstractThinClientTest {

    static class BoolStr {
        @SuppressWarnings("unused")
        boolean boolCol;
        @SuppressWarnings("unused")
        String strCol;
    }

    protected Ignite ignite() {
        return client();
    }

    /** Setup. */
    @BeforeAll
    public void setup() {
        IgniteSql sql = ignite().sql();

        sql.execute(null, "CREATE TABLE t0 (ID INTEGER PRIMARY KEY, boolCol BOOLEAN, strCol VARCHAR)").close();
        sql.execute(null, "INSERT INTO t0 VALUES (1, NULL, NULL)").close();

        sql.execute(null, "CREATE TABLE t1 (ID INTEGER PRIMARY KEY, boolCol BOOLEAN)").close();
        sql.execute(null, "INSERT INTO t1 VALUES (1, NULL)").close();
    }

    @Test
    public void testNullableOpPojo() {
        Table table = ignite().tables().table("T0");
        KeyValueView<Integer, BoolStr> view = table.keyValueView(Mapper.of(Integer.class), Mapper.of(BoolStr.class));

        expectNotSupported(() -> view.getNullable(null, 1));
        expectNotSupported(() -> view.getNullableAndPut(null, 1, null));
        expectNotSupported(() -> view.getNullableAndRemove(null, 1));
        expectNotSupported(() -> view.getNullableAndReplaceAsync(null, 1, null));
    }

    @Test
    public void testModifyOpPoJoNullVal() {
        Table table = ignite().tables().table("T0");
        KeyValueView<Integer, BoolStr> view = table.keyValueView(Mapper.of(Integer.class), Mapper.of(BoolStr.class));

        expectNullNotAllowed(() -> view.put(null, 2, null));
        expectNullNotAllowed(() -> view.putIfAbsent(null, 2, null));
        expectNullNotAllowed(() -> view.replace(null, 2, null));
        expectNullNotAllowed(() -> view.replace(null, 2, new BoolStr(), null));
        expectNullNotAllowed(() -> view.getAndPut(null, 2, null));
        expectNullNotAllowed(() -> view.getAndReplace(null, 2, null));
        expectNullNotAllowed(() -> {
            Map<Integer, BoolStr> map = new HashMap<>();
            map.put(2, null);
            view.putAll(null, map);
        });
    }

    @Test
    public void testNullableOpSingleCol() {
        Table table = ignite().tables().table("T1");
        KeyValueView<Integer, Boolean> view = table.keyValueView(Mapper.of(Integer.class), Mapper.of(Boolean.class));

        assertEquals(NullableValue.of(null), view.getNullable(null, 1));
        assertEquals(NullableValue.of(null), view.getNullableAndPut(null, 1, null));
        assertEquals(NullableValue.of(null), view.getNullableAndRemove(null, 1));
        assertNull(view.getNullableAndReplace(null, 1, null));
    }

    @Test
    public void testModifyOpSingleColNullVal() {
        Table table = ignite().tables().table("T1");
        KeyValueView<Integer, Boolean> view = table.keyValueView(Mapper.of(Integer.class), Mapper.of(Boolean.class));

        assertTrue(view.replace(null, 1, null, false));
        assertEquals(false, view.get(null, 1));

        assertTrue(view.remove(null, 1, false));

        assertTrue(view.putIfAbsent(null, 1, null));
        view.put(null, 1, true);

        assertEquals(true, view.getAndPut(null, 1, null));
        assertNull(view.getAndReplace(null, 2, null));

        Map<Integer, Boolean> map = new HashMap<>();
        map.put(3, null);
        view.putAll(null, map);
        assertEquals(NullableValue.of(null), view.getNullable(null, 3));
    }

    private static void expectNotSupported(Executable exec) {
        UnsupportedOperationException err = assertThrows(UnsupportedOperationException.class, exec);
        assertEquals("`getNullable`* methods cannot be used when a value is not mapped to a simple type", err.getMessage());
    }

    private static void expectNullNotAllowed(Executable exec) {
        NullPointerException err = assertThrows(NullPointerException.class, exec);
        assertEquals("null value cannot be used when a value is not mapped to a simple type", err.getMessage());
    }
}
