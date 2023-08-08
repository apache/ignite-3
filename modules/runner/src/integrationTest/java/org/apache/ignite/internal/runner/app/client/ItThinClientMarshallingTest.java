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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import org.apache.ignite.Ignite;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.Test;

/**
 * Tests marshaller, ensures consistent behavior across client and embedded modes.
 */
@SuppressWarnings("resource")
public class ItThinClientMarshallingTest extends ItAbstractThinClientTest {
    protected Ignite ignite() {
        return client();
    }

    @Test
    public void testUnmappedPojoFields() {
        Table table = ignite().tables().table(TABLE_NAME);
        var pojoView = table.recordView(TestPojo2.class);

        var pojo = new TestPojo2();
        pojo.key = 1;
        pojo.val = "val";
        pojo.unmapped = "unmapped";

        Throwable ex = assertThrowsWithCause(() -> pojoView.upsert(null, pojo), IllegalArgumentException.class);
        assertEquals(
                "Fields [unmapped, unmapped2] of type "
                        + "org.apache.ignite.internal.runner.app.client.ItThinClientMarshallingTest$TestPojo2 are not mapped to columns.",
                ex.getMessage());
    }

    @Test
    public void testKvUnmappedKeyPojoFields() {
        Table table = ignite().tables().table(TABLE_NAME);
        var kvPojoView = table.keyValueView(TestPojo.class, TestPojo.class);

        var pojo = new TestPojo();

        Throwable ex = assertThrowsWithCause(() -> kvPojoView.put(null, pojo, pojo), IllegalArgumentException.class);
        assertEquals(
                "Fields [val] of type org.apache.ignite.internal.runner.app.client.ItAbstractThinClientTest$TestPojo "
                        + "are not mapped to columns.",
                ex.getMessage());
    }

    @Test
    public void testKvUnmappedValPojoFields() {
        Table table = ignite().tables().table(TABLE_NAME);
        var kvPojoView = table.keyValueView(Integer.class, TestPojo.class);

        var pojo = new TestPojo();

        Throwable ex = assertThrowsWithCause(() -> kvPojoView.put(null, 1, pojo), IllegalArgumentException.class);
        assertEquals(
                "Fields [key] of type org.apache.ignite.internal.runner.app.client.ItAbstractThinClientTest$TestPojo "
                        + "are not mapped to columns.",
                ex.getMessage());
    }

    @Test
    public void testUnmappedTupleFields() {
        Table table = ignite().tables().table(TABLE_NAME);
        var tupleView = table.recordView();

        var tuple = Tuple.create().set("key", 1).set("val", "val").set("unmapped", "unmapped");

        Throwable ex = assertThrowsWithCause(() -> tupleView.upsert(null, tuple), IgniteException.class);
        assertEquals("Tuple doesn't match schema: schemaVersion=1, extraColumns=[UNMAPPED]", ex.getMessage());
    }

    @Test
    public void testKvUnmappedKeyTupleFields() {
        Table table = ignite().tables().table(TABLE_NAME);
        var tupleView = table.keyValueView();

        var tuple = Tuple.create().set("key", 1).set("val", "val");

        Throwable ex = assertThrowsWithCause(() -> tupleView.put(null, tuple, tuple), IgniteException.class);
        assertEquals("Key tuple doesn't match schema: schemaVersion=1, extraColumns=[VAL]", ex.getMessage());
    }

    @Test
    public void testKvUnmappedValTupleFields() {
        Table table = ignite().tables().table(TABLE_NAME);
        var tupleView = table.keyValueView();

        var key = Tuple.create().set("key", 1);
        var tuple = Tuple.create().set("key", 1).set("val", "val");

        Throwable ex = assertThrowsWithCause(() -> tupleView.put(null, key, tuple), IgniteException.class);
        assertEquals("Value tuple doesn't match schema: schemaVersion=1, extraColumns=[KEY]", ex.getMessage());
    }
    
    @Test
    public void testMissingPojoFields() {
        Table table = ignite().tables().table(TABLE_NAME);
        var pojoView = table.recordView(MissingFieldPojo.class);

        Throwable ex = assertThrowsWithCause(() -> pojoView.upsert(null, new MissingFieldPojo()), IllegalArgumentException.class);
        assertEquals("No field found for column KEY", ex.getMessage());
    }

    @Test
    public void testKvMissingKeyPojoFields() {
        Table table = ignite().tables().table(TABLE_NAME);
        var kvPojoView = table.keyValueView(MissingFieldPojo.class, String.class);

        Throwable ex = assertThrowsWithCause(() -> kvPojoView.put(null, new MissingFieldPojo(), ""), IllegalArgumentException.class);
        assertEquals("No field found for column KEY", ex.getMessage());
    }

    @Test
    public void testKvMissingValPojoFields() {
        Table table = ignite().tables().table(TABLE_NAME);
        var kvPojoView = table.keyValueView(Integer.class, MissingFieldPojo.class);

        Throwable ex = assertThrowsWithCause(() -> kvPojoView.put(null, 1, new MissingFieldPojo()), IllegalArgumentException.class);
        assertEquals("No field found for column VAL", ex.getMessage());
    }

    @Test
    public void testMissingKeyTupleFields() {
        Table table = ignite().tables().table(TABLE_NAME);
        var tupleView = table.recordView();

        Throwable ex = assertThrowsWithCause(() -> tupleView.upsert(null, Tuple.create()), IgniteException.class);
        assertEquals("Missed key column: KEY", ex.getMessage());
    }

    @Test
    public void testMissingValTupleFields() {
        var tableName = "testMissingValTupleFields";
        ignite().sql().createSession().execute(null, "CREATE TABLE " + tableName + " (KEY INT PRIMARY KEY, VAL VARCHAR NOT NULL)");

        Table table = ignite().tables().table(tableName);
        var tupleView = table.recordView();

        Throwable ex = assertThrowsWithCause(() -> tupleView.upsert(null, Tuple.create().set("KEY", 1)), IgniteException.class);
        assertThat(ex.getMessage(), startsWith(
                "Failed to set column (null was passed, but column is not nullable): [col=Column [schemaIndex=1, columnOrder=1, name=VAL"));
    }

    @Test
    public void testKvMissingKeyTupleFields() {
        Table table = ignite().tables().table(TABLE_NAME);
        var tupleView = table.keyValueView();

        Throwable ex = assertThrowsWithCause(() -> tupleView.put(null, Tuple.create(), Tuple.create()), IgniteException.class);
        assertEquals("Missed key column: KEY", ex.getMessage());
    }

    @Test
    public void testKvMissingValTupleFields() {
        var tableName = "testKvMissingValTupleFields";
        ignite().sql().createSession().execute(null, "CREATE TABLE " + tableName + " (KEY INT PRIMARY KEY, VAL VARCHAR NOT NULL)");

        Table table = ignite().tables().table(tableName);
        var tupleView = table.keyValueView();

        Throwable ex = assertThrowsWithCause(
                () -> tupleView.put(null, Tuple.create().set("KEY", 1), Tuple.create()),
                IgniteException.class);

        assertThat(ex.getMessage(), startsWith(
                "Failed to set column (null was passed, but column is not nullable): [col=Column [schemaIndex=1, columnOrder=1, name=VAL"));
    }

    @Test
    public void testMissingTupleFieldsWithDefaultValue() {
        var tableName = "testMissingTupleFieldsWithDefaultValue";
        ignite().sql().createSession().execute(null,
                "CREATE TABLE " + tableName + " (KEY INT PRIMARY KEY, VAL VARCHAR NOT NULL DEFAULT 'def')");

        Table table = ignite().tables().table(tableName);
        var tupleView = table.recordView();

        tupleView.upsert(null, Tuple.create().set("KEY", 1));
        assertEquals("def", tupleView.get(null, Tuple.create().set("KEY", 1)).value("VAL"));
    }

    @Test
    public void testIncompatiblePojoFieldType() {
        Table table = ignite().tables().table(TABLE_NAME);
        var pojoView = table.recordView(Mapper.of(IncompatibleFieldPojo.class));

        IncompatibleFieldPojo rec = new IncompatibleFieldPojo();
        rec.key = "1";
        rec.val = BigDecimal.ONE;

        Throwable ex = assertThrowsWithCause(() -> pojoView.upsert(null, rec), ClassCastException.class);
        assertThat(ex.getMessage(), startsWith("class java.math.BigDecimal cannot be cast to class java.lang.CharSequence"));
    }

    @Test
    public void testIncompatibleTupleElementType() {
        Table table = ignite().tables().table(TABLE_NAME);
        var tupleView = table.recordView();

        Tuple rec = Tuple.create().set("KEY", "1").set("VAL", BigDecimal.ONE);

        Throwable ex = assertThrows(IgniteException.class, () -> tupleView.upsert(null, rec));
        assertThat(ex.getMessage(), anyOf(startsWith("Column's type mismatch"), startsWith("Incorrect value type for column 'KEY'")));
    }

    // TODO: Add schema update tests - add column, drop column.

    private static class TestPojo2 {
        public int key;

        public String val;

        public String unmapped;

        public String unmapped2;
    }

    private static class MissingFieldPojo {
        public int unknown;
    }

    private static class IncompatibleFieldPojo {
        public String key;
        public BigDecimal val;
    }
}
