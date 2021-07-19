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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClientTableTest extends AbstractClientTest {
    @Test
    public void testPutGet() {
        server.tables().createTable(DEFAULT_TABLE, tbl -> tbl.changeReplicas(1));

        var table = client.tables().table(DEFAULT_TABLE);

        var tuple = table.tupleBuilder()
                .set("accountNumber", "123")
                .set("firstName", "John")
                .build();

        var insertRes = table.insert(tuple);

        Tuple keyTuple = table.tupleBuilder().set("accountNumber", "123").build();
        var resTuple = table.get(keyTuple);

        assertTrue(insertRes);
        assertEquals(tuple, resTuple);
    }

    @Test
    public void testPutGetAsync() {
        // TODO
    }
}
