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

package org.apache.ignite.internal.client.table;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.ignite.internal.marshaller.ReflectionMarshallersProvider;
import org.apache.ignite.sql.ColumnType;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.Test;

/**
 * Tests for colocation hash functionality in {@link ClientTupleSerializer}.
 */
public class ColocationHashTests {
    private static final ReflectionMarshallersProvider marshallers = new ReflectionMarshallersProvider();

    // Key:        key1, key2
    // Colocation: key2, key1
    private static final ClientSchema SCHEMA = new ClientSchema(1, new ClientColumn[]{
            new ClientColumn("VAL1", ColumnType.INT32, false, -1, 0, -1, 0),
            new ClientColumn("KEY1", ColumnType.INT32, false, 0, -1, 1, 1),
            new ClientColumn("VAL2", ColumnType.INT32, false, -1, 1, -1, 2),
            new ClientColumn("KEY2", ColumnType.INT32, false, 1, -1, 0, 3),
    }, marshallers);

    @Test
    public void testPojoInterleavedKeyAndValueColumns() {
        var pojo = new Pojo();
        pojo.val1 = 1;
        pojo.key1 = 2;
        pojo.val2 = 3;
        pojo.key2 = 4;

        var keyPojo = new Pojo();
        keyPojo.key1 = pojo.key1;
        keyPojo.key2 = pojo.key2;

        var tuple = Tuple.create()
                .set("VAL1", pojo.val1)
                .set("KEY1", pojo.key1)
                .set("VAL2", pojo.val2)
                .set("KEY2", pojo.key2);

        var keyTuple = Tuple.create()
                .set("KEY1", pojo.key1)
                .set("KEY2", pojo.key2);

        Integer tupleHash = ClientTupleSerializer.getColocationHash(SCHEMA, tuple);
        Integer keyTupleHash = ClientTupleSerializer.getColocationHash(SCHEMA, keyTuple);
        Integer pojoHash = ClientTupleSerializer.getColocationHash(SCHEMA, Mapper.of(Pojo.class), pojo);
        Integer keyPojoHash = ClientTupleSerializer.getColocationHash(SCHEMA, Mapper.of(Pojo.class), keyPojo);

        assertEquals(tupleHash, keyTupleHash);
        assertEquals(tupleHash, pojoHash);
        assertEquals(tupleHash, keyPojoHash);
    }

    static class Pojo {
        int val1;
        int key1;
        int val2;
        int key2;
    }
}
