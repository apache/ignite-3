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

import org.apache.ignite.table.KeyValueBinaryView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * KeyValueBinaryView tests.
 */
public class ClientKeyValueBinaryViewTest extends AbstractClientTableTest {
    @Test
    public void testGetMissingRowReturnsNull() {
        Table table = defaultTable();
        KeyValueBinaryView kvView = table.kvView();

        assertNull(kvView.get(defaultTupleKey(table)));
    }

    @Test
    public void testTableUpsertKvGet() {
        Table table = defaultTable();
        table.upsert(tuple());

        KeyValueBinaryView kvView = table.kvView();

        Tuple key = defaultTupleKey(table);
        Tuple val = kvView.get(key);

        assertEquals(DEFAULT_NAME, val.value("name"));
        assertEquals(DEFAULT_NAME, val.value(0));
        assertEquals(1, val.columnCount());
    }
}
