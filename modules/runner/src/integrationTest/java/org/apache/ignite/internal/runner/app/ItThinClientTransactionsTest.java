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
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.Test;

/**
 * Thin client transactions integration test.
 */
public class ItThinClientTransactionsTest extends ItThinClientAbstractTest {
    /**
     * Check that thin client can connect to any server node and work with table API.
     */
    @Test
    void testTransactionCommitRollback() throws Exception {
        try (var client = IgniteClient.builder().addresses(getNodeAddresses().get(0)).build()) {
            Table table = client.tables().tables().get(0);
            KeyValueView<Integer, String> kvView = table.keyValueView(Mapper.of(Integer.class), Mapper.of(String.class));
            kvView.put(null, 1, "1");

            Transaction tx = client.transactions().begin();
            assertEquals("1", kvView.get(tx, 1));

            fail("TODO");
        }
    }
}
