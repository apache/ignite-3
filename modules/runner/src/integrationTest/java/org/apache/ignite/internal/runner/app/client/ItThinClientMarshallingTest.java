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
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.ignite.Ignite;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
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
    public void testUnmappedPojoFieldsAreRejected() {
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
    public void testKvUnmappedKeyPojoFieldsAreRejected() {
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
    public void testKvUnmappedValPojoFieldsAreRejected() {
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
    public void testUnmappedTupleFieldsAreRejected() {
        Table table = ignite().tables().table(TABLE_NAME);
        var tupleView = table.recordView();

        var tuple = Tuple.create().set("key", 1).set("val", "val").set("unmapped", "unmapped");

        Throwable ex = assertThrowsWithCause(() -> tupleView.upsert(null, tuple), IgniteException.class);
        assertEquals("Tuple doesn't match schema: schemaVersion=1, extraColumns=[UNMAPPED]", ex.getMessage());
    }

    @Test
    public void testKvUnmappedKeyTupleFieldsAreRejected() {
        Table table = ignite().tables().table(TABLE_NAME);
        var tupleView = table.keyValueView();

        var tuple = Tuple.create().set("key", 1).set("val", "val");

        Throwable ex = assertThrowsWithCause(() -> tupleView.put(null, tuple, tuple), IgniteException.class);
        assertEquals("Key tuple doesn't match schema: schemaVersion=1, extraColumns=[VAL]", ex.getMessage());
    }

    @Test
    public void testKvUnmappedValTupleFieldsAreRejected() {
        Table table = ignite().tables().table(TABLE_NAME);
        var tupleView = table.keyValueView();

        var key = Tuple.create().set("key", 1);
        var tuple = Tuple.create().set("key", 1).set("val", "val");

        Throwable ex = assertThrowsWithCause(() -> tupleView.put(null, key, tuple), IgniteException.class);
        assertEquals("Value tuple doesn't match schema: schemaVersion=1, extraColumns=[KEY]", ex.getMessage());
    }

    private static class TestPojo2 {
        public int key;

        public String val;

        public String unmapped;

        public String unmapped2;
    }
}
