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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCode;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import org.apache.ignite.Ignite;
import org.apache.ignite.lang.ErrorGroups.Marshalling;
import org.apache.ignite.lang.MarshallerException;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

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

        assertThrowsMarshallerException(
                () -> pojoView.upsert(null, pojo),
                "Fields [unmapped, unmapped2] of type "
                        + "org.apache.ignite.internal.runner.app.client.ItThinClientMarshallingTest$TestPojo2 are not mapped to columns"
        );
    }

    @Test
    public void testKvUnmappedKeyPojoFields() {
        Table table = ignite().tables().table(TABLE_NAME);
        var kvPojoView = table.keyValueView(TestPojo.class, TestPojo.class);

        var pojo = new TestPojo();

        assertThrowsMarshallerException(
                () -> kvPojoView.put(null, pojo, pojo),
                "Fields [val] of type org.apache.ignite.internal.runner.app.client.ItAbstractThinClientTest$TestPojo "
                        + "are not mapped to columns");
    }

    @Test
    public void testKvUnmappedValPojoFields() {
        Table table = ignite().tables().table(TABLE_NAME);
        var kvPojoView = table.keyValueView(Integer.class, TestPojo.class);

        var pojo = new TestPojo();

        assertThrowsMarshallerException(
                () -> kvPojoView.put(null, 1, pojo),
                "Fields [key] of type org.apache.ignite.internal.runner.app.client.ItAbstractThinClientTest$TestPojo "
                        + "are not mapped to columns"
        );
    }

    @Test
    public void testUnmappedTupleFields() {
        Table table = ignite().tables().table(TABLE_NAME);
        var tupleView = table.recordView();

        var tuple = Tuple.create().set("key", 1).set("val", "val").set("unmapped", "unmapped");

        assertThrowsMarshallerException(
                () -> tupleView.upsert(null, tuple),
                "Tuple doesn't match schema: schemaVersion=1, extraColumns=[UNMAPPED]");
    }

    @Test
    public void testKvUnmappedKeyTupleFields() {
        Table table = ignite().tables().table(TABLE_NAME);
        var tupleView = table.keyValueView();

        var tuple = Tuple.create().set("key", 1).set("val", "val");

        assertThrowsMarshallerException(
                () -> tupleView.put(null, tuple, tuple),
                "Key tuple doesn't match schema: schemaVersion=1, extraColumns=[VAL]");
    }

    @Test
    public void testKvUnmappedValTupleFields() {
        Table table = ignite().tables().table(TABLE_NAME);
        var tupleView = table.keyValueView();

        var key = Tuple.create().set("key", 1);
        var tuple = Tuple.create().set("key", 1).set("val", "val");

        assertThrowsMarshallerException(
                () -> tupleView.put(null, key, tuple),
                "Value tuple doesn't match schema: schemaVersion=1, extraColumns=[KEY]");
    }

    @Test
    public void testMissingPojoFields() {
        Table table = ignite().tables().table(TABLE_NAME);
        var pojoView = table.recordView(MissingFieldPojo.class);

        assertThrowsMarshallerException(
                () -> pojoView.upsert(null, new MissingFieldPojo()),
                "No mapped object field found for column 'KEY'");
    }

    @Test
    public void testKvMissingKeyPojoFields() {
        Table table = ignite().tables().table(TABLE_NAME);
        var kvPojoView = table.keyValueView(MissingFieldPojo.class, String.class);

        assertThrowsMarshallerException(
                () -> kvPojoView.put(null, new MissingFieldPojo(), ""),
                "No mapped object field found for column 'KEY'");
    }

    @Test
    public void testKvMissingValPojoFields() {
        String tableName = "tableWithExtraField";
        ignite().sql().execute(null, "CREATE TABLE " + tableName + " (KEY INT PRIMARY KEY, VAL VARCHAR, EXTRA VARCHAR)");
        Table table = ignite().tables().table(tableName);

        var kvPojoView = table.keyValueView(Integer.class, MissingFieldPojo2.class);

        kvPojoView.put(null, 1, new MissingFieldPojo2("x"));
        MissingFieldPojo2 val = kvPojoView.get(null, 1);

        assertEquals("x", val.val);
    }

    @Test
    public void testMissingKeyTupleFields() {
        Table table = ignite().tables().table(TABLE_NAME);
        var tupleView = table.recordView();

        assertThrowsMarshallerException(
                () -> tupleView.upsert(null, Tuple.create()),
                "Missed key column: KEY");
    }

    @Test
    public void testMissingValTupleFields() {
        var tableName = "testMissingValTupleFields";
        ignite().sql().execute(null, "CREATE TABLE " + tableName + " (KEY INT PRIMARY KEY, VAL VARCHAR NOT NULL)");

        Table table = ignite().tables().table(tableName);
        var tupleView = table.recordView();

        assertThrowsMarshallerException(
                () -> tupleView.upsert(null, Tuple.create().set("KEY", 1)),
                "Column 'VAL' does not allow NULLs");
    }

    @Test
    public void testKvMissingKeyTupleFields() {
        Table table = ignite().tables().table(TABLE_NAME);
        var tupleView = table.keyValueView();

        assertThrowsMarshallerException(
                () -> tupleView.put(null, Tuple.create(), Tuple.create()),
                "Missed key column: KEY");
    }

    @Test
    public void testKvMissingValTupleFields() {
        var tableName = "testKvMissingValTupleFields";
        ignite().sql().execute(null, "CREATE TABLE " + tableName + " (KEY INT PRIMARY KEY, VAL VARCHAR NOT NULL)");

        Table table = ignite().tables().table(tableName);
        var tupleView = table.keyValueView();

        assertThrowsMarshallerException(
                () -> tupleView.put(null, Tuple.create().set("KEY", 1), Tuple.create()),
                "Column 'VAL' does not allow NULLs");
    }

    @Test
    public void testMissingTupleFieldsWithDefaultValue() {
        var tableName = "testMissingTupleFieldsWithDefaultValue";
        ignite().sql().execute(null,
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

        assertThrowsMarshallerException(
                () -> pojoView.upsert(null, rec),
                "Column's type mismatch");
    }

    @Test
    public void testIncompatiblePojoFieldType2() {
        Table table = ignite().tables().table(TABLE_NAME);
        var pojoView = table.recordView(Mapper.of(IncompatibleFieldPojo2.class));

        IncompatibleFieldPojo2 rec = new IncompatibleFieldPojo2();
        rec.key = -1;
        rec.val = "f";

        assertThrowsMarshallerException(
                () -> pojoView.upsert(null, rec),
                "Column's type mismatch");
    }

    @Test
    public void testIncompatibleTupleElementType() {
        var tableName = "testIncompatibleTupleElementType";
        ignite().sql().execute(null, "CREATE TABLE " + tableName + " (KEY INT PRIMARY KEY, VAL VARCHAR NOT NULL)");

        Table table = ignite().tables().table(tableName);
        var tupleView = table.recordView();

        Tuple rec = Tuple.create().set("KEY", 1).set("VAL", 1L);

        // TODO: https://issues.apache.org/jira/browse/IGNITE-22965.
        // The validation done on a client side (for a thin client), and messages may differ between embedded clients and thin clients.
        // For an embedded client the message include type precision, but for a thin client it doesn't.
        MarshallerException ex = assertThrows(MarshallerException.class, () -> tupleView.upsert(null, rec));
        assertEquals(Marshalling.COMMON_ERR, ex.code());
        assertThat(ex.getMessage(), containsString("Value type does not match [column='VAL', expected=STRING"));
        assertThat(ex.getMessage(), endsWith(", actual=INT64]"));
    }

    @Test
    public void testBoxedPrimitivePojo() {
        Table table = ignite().tables().table(TABLE_NAME);
        var pojoView = table.recordView(Mapper.of(BoxedPrimitivePojo.class));

        BoxedPrimitivePojo rec = new BoxedPrimitivePojo();
        rec.key = -1;
        rec.val = "f";

        pojoView.upsert(null, rec);
        assertEquals("f", pojoView.get(null, rec).val);
    }

    @Test
    public void testBoxedPrimitiveTuple() {
        Table table = ignite().tables().table(TABLE_NAME);
        var tupleView = table.recordView();

        Integer key = -1;
        Tuple rec = Tuple.create().set("KEY", key).set("VAL", "v");

        tupleView.upsert(null, rec);
        assertEquals("v", tupleView.get(null, Tuple.create().set("KEY", key)).value("VAL"));
    }

    @Test
    public void testNullKeyTupleFields() {
        Table table = ignite().tables().table(TABLE_NAME);
        var tupleView = table.recordView();

        assertThrowsMarshallerException(
                () -> tupleView.upsert(null, Tuple.create().set("KEY", null)),
                "Column 'KEY' does not allow NULLs");
    }

    @Test
    public void testNullValTupleFields() {
        var tableName = "testNullValTupleFields";
        ignite().sql().execute(null, "CREATE TABLE " + tableName + " (KEY INT PRIMARY KEY, VAL VARCHAR NOT NULL)");

        Table table = ignite().tables().table(tableName);
        var tupleView = table.recordView();

        Tuple rec = Tuple.create().set("KEY", 1).set("VAL", null);
        assertThrowsMarshallerException(
                () -> tupleView.upsert(null, rec),
                "Column 'VAL' does not allow NULLs");
    }

    @Test
    public void testKvNullKeyTupleFields() {
        Table table = ignite().tables().table(TABLE_NAME);
        var tupleView = table.keyValueView();

        assertThrowsMarshallerException(
                () -> tupleView.put(null, Tuple.create(), Tuple.create()),
                "Missed key column: KEY");
    }

    @Test
    public void testKvNullValTupleFields() {
        var tableName = "testKvNullValTupleFields";
        ignite().sql().execute(null, "CREATE TABLE " + tableName + " (KEY INT PRIMARY KEY, VAL VARCHAR NOT NULL)");

        Table table = ignite().tables().table(tableName);
        var tupleView = table.keyValueView();

        assertThrowsMarshallerException(
                () -> tupleView.put(null, Tuple.create().set("KEY", 1), Tuple.create().set("VAL", null)),
                "Column 'VAL' does not allow NULLs");
    }

    @Test
    public void testVarcharColumnOverflow() {
        var tableName = "testVarcharColumnOverflow";

        ignite().sql().execute(null, "CREATE TABLE " + tableName + " (KEY INT PRIMARY KEY, VAL VARCHAR(10))");

        Table table = ignite().tables().table(tableName);
        var tupleView = table.keyValueView();

        assertThrowsMarshallerException(
                () -> tupleView.put(null, Tuple.create().set("KEY", 1), Tuple.create().set("VAL", "1".repeat(20))),
                "Value too long [column='VAL', type=STRING(10)]");
    }

    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-22965")
    public void testDecimalColumnOverflow() {
        var tableName = "testDecimalColumnOverflow";

        ignite().sql().execute(null, "CREATE TABLE " + tableName + " (KEY INT PRIMARY KEY, VAL DECIMAL(3,1))");

        Table table = ignite().tables().table(tableName);
        var tupleView = table.keyValueView();

        assertThrowsMarshallerException(
                () -> tupleView.put(null, Tuple.create().set("KEY", 1), Tuple.create().set("VAL", new BigDecimal("12345.1"))),
                "Numeric field overflow in column 'VAL'");
    }

    @Test
    public void testUnsupportedObjectInTuple() {
        Table table = ignite().tables().table(TABLE_NAME);
        RecordView<Tuple> tupleView = table.recordView();

        Tuple rec = Tuple.create()
                .set("KEY", 1)
                .set("VAL", new TestPojo2());

        assertThrowsMarshallerException(
                () -> tupleView.upsert(null, rec),
                "Invalid value type provided for column [name='VAL', expected='java.lang.String', actual='"
                        + TestPojo2.class.getName() + "']"
        );
    }

    private static void assertThrowsMarshallerException(Executable run, String messageFragment) {
        assertThrowsWithCode(MarshallerException.class, Marshalling.COMMON_ERR, run, messageFragment);
    }

    private static class TestPojo2 {
        public int key;

        public String val;

        public String unmapped;

        public String unmapped2;

        public static String staticField;

        public transient String transientField;
    }

    private static class MissingFieldPojo {
        public int unknown;
    }

    private static class MissingFieldPojo2 {
        public String val;

        MissingFieldPojo2() {
            // No-op.
        }

        public MissingFieldPojo2(String val) {
            this.val = val;
        }
    }

    private static class IncompatibleFieldPojo {
        public String key; // Must be int.
        public BigDecimal val;
    }

    private static class IncompatibleFieldPojo2 {
        public short key; // Must be int.
        public String val;
    }

    private static class BoxedPrimitivePojo {
        public Integer key;
        public String val;
    }
}
