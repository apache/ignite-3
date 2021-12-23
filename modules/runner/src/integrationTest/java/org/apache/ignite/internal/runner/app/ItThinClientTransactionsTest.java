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

package org.apache.ignite.internal.runner.app;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.Test;

/**
 * Thin client transactions integration test.
 */
// TODO: Test ALL operations in ALL modes (tuple, kv, binary).
// TODO: Test invalid use cases (closed tx usage, invalid interface usage).
public class ItThinClientTransactionsTest extends ItThinClientAbstractTest {
    @Test
    void testCommit() {
        KeyValueView<Integer, String> kvView = kvView();
        kvView.put(null, 1, "1");

        Transaction tx = client().transactions().begin();
        assertEquals("1", kvView.get(null, 1));
        assertEquals("1", kvView.get(tx, 1));

        kvView.put(tx, 1, "2");
        assertEquals("2", kvView.get(tx, 1));

        tx.commit();
        assertEquals("2", kvView.get(null, 1));
    }

    @Test
    void testRollback() {
        KeyValueView<Integer, String> kvView = kvView();
        kvView.put(null, 1, "1");

        Transaction tx = client().transactions().begin();
        assertEquals("1", kvView.get(null, 1));
        assertEquals("1", kvView.get(tx, 1));

        kvView.put(tx, 1, "2");
        assertEquals("2", kvView.get(tx, 1));

        tx.rollback();
        assertEquals("1", kvView.get(null, 1));
    }

    @Test
    void testCommitTwice() {
        // TODO
    }

    private KeyValueView<Integer, String> kvView() {
        Table table = client().tables().tables().get(0);

        return table.keyValueView(Mapper.of(Integer.class), Mapper.of(String.class));
    }
}
