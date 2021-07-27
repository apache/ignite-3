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

package org.apache.ignite.client;

import org.apache.ignite.client.proto.ClientDataType;
import org.apache.ignite.internal.client.table.ClientColumn;
import org.apache.ignite.internal.client.table.ClientSchema;
import org.apache.ignite.internal.client.table.ClientTupleBuilder;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests client tuple builder implementation.
 */
public class ClientTupleBuilderTest {
    private static final ClientSchema SCHEMA = new ClientSchema(1, new ClientColumn[]{
            new ClientColumn("id", ClientDataType.INT64, false, true, 0),
            new ClientColumn("name", ClientDataType.STRING, false, false, 1),
            new ClientColumn("size", ClientDataType.INT32, true, false, 2)
    });

    @Test
    public void testEmptySchemaThrows() {
        assertThrows(AssertionError.class, () -> new ClientTupleBuilder(new ClientSchema(1, new ClientColumn[0])));
    }

    @Test
    public void testSetThrowsWhenColumnIsNotPresent() {
        var ex = assertThrows(IgniteException.class, () -> getBuilder().set("x", "y"));
        assertEquals("Column is not present in schema: x", ex.getMessage());
    }

    @Test
    public void testValueThrowsWhenColumnIsNotPresent() {
        var ex = assertThrows(IgniteException.class, () -> getBuilder().value("x"));
        assertEquals("Column is not present in schema: x", ex.getMessage());

        var ex2 = assertThrows(IllegalArgumentException.class, () -> getBuilder().value(100));
        assertEquals("Column index can't be greater than 2", ex2.getMessage());
    }

    @Test
    public void testValueOrDefaultReturnsDefaultWhenColumnIsNotPresent() {
        assertEquals("foo", getBuilder().valueOrDefault("x", "foo"));
    }

    @Test
    public void testValueOrDefaultReturnsNullWhenColumnIsPresentButNotSet() {
        assertNull(getBuilder().valueOrDefault("name", "foo"));
    }

    @Test
    public void testColumnCountReturnsSchemaSize() {
        assertEquals(SCHEMA.columns().length, getTuple().columnCount());
    }

    @Test
    public void testColumnNameReturnsNameByIndex() {
        assertEquals("id", getTuple().columnName(0));
        assertEquals("name", getTuple().columnName(1));
        assertEquals("size", getTuple().columnName(2));
    }

    private static ClientTupleBuilder getBuilder() {
        return new ClientTupleBuilder(SCHEMA);
    }

    private static Tuple getTuple() {
        return new ClientTupleBuilder(SCHEMA)
                .set("id", 3L)
                .set("name", "Shirt")
                .set("size", 99)
                .build();
    }
}
