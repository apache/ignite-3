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

import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientTableTest extends AbstractClientTest {
    @Test
    public void testGetWithNullInNotNullableKeyColumnThrowsException() {
        Table table = getDefaultTable();

        var key = table.tupleBuilder().set("foo", "123").build();

        var ex = assertThrows(CompletionException.class, () -> table.get(key));

        assertTrue(ex.getMessage().contains("Failed to set column (null was passed, but column is not nullable)"));
    }

    @Test
    public void testInsertGet() {
        Table table = getDefaultTable();

        var tuple = table.tupleBuilder()
                .set("id", 123)
                .set("name", "John")
                .build();

        var insertRes = table.insert(tuple);

        Tuple key = table.tupleBuilder().set("id", 123).build();
        var resTuple = table.get(key);

        assertTrue(insertRes);
        assertEquals(tuple, resTuple);
    }

    @Test
    public void testPutGetAsync() {
        // TODO
    }

    private Table getDefaultTable() {
        server.tables().getOrCreateTable(DEFAULT_TABLE, tbl -> tbl.changeReplicas(1));

        return client.tables().table(DEFAULT_TABLE);
    }
}
