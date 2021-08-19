/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.table;

import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests server tuple builder implementation.
 *
 * Should be in sync with org.apache.ignite.client.ClientTupleBuilderTest.
 */
public class TupleImplTest {
    @Test
    public void testValueReturnsValueByName() {
        assertEquals(3L, (Long) getTuple().value("id"));
        assertEquals("Shirt", getTuple().value("name"));
    }

    @Test
    public void testValueThrowsOnInvalidColumnName() {
        var ex = assertThrows(IllegalArgumentException.class, () -> getTuple().value("x"));
        assertEquals("Column not found: columnName=x", ex.getMessage());
    }

    @Test
    public void testValueReturnsValueByIndex() {
        assertEquals(3L, (Long) getTuple().value(0));
        assertEquals("Shirt", getTuple().value(1));
    }

    @Test
    public void testValueThrowsOnInvalidIndex() {
        var ex = assertThrows(IndexOutOfBoundsException.class, () -> getTuple().value(-1));
        assertEquals("Index -1 out of bounds for length 2", ex.getMessage());

        ex = assertThrows(IndexOutOfBoundsException.class, () -> getTuple().value(3));
        assertEquals("Index 3 out of bounds for length 2", ex.getMessage());
    }

    @Test
    public void testValueOrDefaultReturnsValueByName() {
        assertEquals(3L, getTuple().valueOrDefault("id", -1L));
        assertEquals("Shirt", getTuple().valueOrDefault("name", "y"));
    }

    @Test
    public void testValueOrDefaultReturnsDefaultWhenColumnIsNotSet() {
        assertEquals("foo", createTuple().valueOrDefault("x", "foo"));
    }

    @Test
    public void testValueReturnsOverwrittenValue() {
        assertEquals("foo", createTuple().set("name", "foo").value("name"));
        assertEquals("foo", createTuple().set("name", "foo").valueOrDefault("name", "bar"));
    }

    @Test
    public void testValueOrDefaultReturnsNullWhenColumnIsSetToNull() {
        var tuple = createTuple().set("name", null);

        assertNull(tuple.valueOrDefault("name", "foo"));
    }

    @Test
    public void testColumnCountReturnsSchemaSize() {
        assertEquals(0, createTuple().columnCount());

        Tuple tuple = getTuple();

        assertEquals(2, tuple.columnCount());
        assertEquals(2, tuple.set("id", -1).columnCount());

        tuple.valueOrDefault("name", "foo");
        assertEquals(2, tuple.columnCount());

        tuple.valueOrDefault("foo", "bar");
        assertEquals(2, tuple.columnCount());

        tuple.set("foo", "bar");
        assertEquals(3, tuple.columnCount());
    }

    @Test
    public void testColumnNameReturnsNameByIndex() {
        assertEquals("id", getTuple().columnName(0));
        assertEquals("name", getTuple().columnName(1));
    }

    @Test
    public void testColumnNameThrowsOnInvalidIndex() {
        var ex = assertThrows(IndexOutOfBoundsException.class, () -> getTuple().columnName(-1));
        assertEquals("Index -1 out of bounds for length 2", ex.getMessage());

        ex = assertThrows(IndexOutOfBoundsException.class, () -> getTuple().columnName(3));
        assertEquals("Index 3 out of bounds for length 2", ex.getMessage());
    }

    @Test
    public void testColumnIndexReturnsIndexByName() {
        assertEquals(0, getTuple().columnIndex("id"));
        assertEquals(1, getTuple().columnIndex("name"));
    }

    @Test
    public void testColumnIndexForMissingColumns() {
        assertEquals(-1, getTuple().columnIndex("foo"));
    }

    private static TupleImpl createTuple() {
        return new TupleImpl();
    }

    private static Tuple getTuple() {
        return new TupleImpl()
                .set("id", 3L)
                .set("name", "Shirt");
    }
}
