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

package org.apache.ignite.internal.tx;

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteTransactionsImpl;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.tx.impl.IgniteTransactionsImpl;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.Test;

/**
 * Test concurrent sorted index creation with nonunique index.
 */
public class ItTransactionsInsertLockTest extends ClusterPerTestIntegrationTest {

    private static final String TABLE_NAME = "indexed_tbl";

    private static final String INDEX_NAME = "indexed_tbl_index";

    @Test
    void testBiggerFirst() {
        executeSql("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, name VARCHAR)");
        executeSql("CREATE INDEX " + INDEX_NAME + " on " + TABLE_NAME + " using SORTED(name)");

        KeyValueView<Integer, String> keyValueView = node(0).tables().table(TABLE_NAME).keyValueView(Integer.class, String.class);

        executeSql("INSERT INTO " + TABLE_NAME + " (id, name) VALUES(1, 'Europe')");

        IgniteTransactionsImpl txns0 = unwrapIgniteTransactionsImpl(node(0).transactions());

        Transaction tx0 = txns0.begin();

        keyValueView.put(tx0, 9, "Oceania");

        Transaction tx1 = txns0.begin();

        keyValueView.put(tx1, 6, "Oceania");

        tx0.commit();
        tx1.commit();

        List<List<Object>> sql = executeSql("SELECT * FROM " + TABLE_NAME);

        assertEquals(3, sql.size());
    }
}
