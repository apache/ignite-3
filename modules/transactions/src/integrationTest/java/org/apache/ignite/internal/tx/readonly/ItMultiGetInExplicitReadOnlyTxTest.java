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

package org.apache.ignite.internal.tx.readonly;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionOptions;
import org.junit.jupiter.api.Test;

class ItMultiGetInExplicitReadOnlyTxTest extends ClusterPerTestIntegrationTest {
    private static final String TABLE_NAME = "TEST_TABLE";

    private static final int KEY_COUNT = 100;

    @Override
    protected int initialNodes() {
        return 2;
    }

    @Test
    void roTransactionWithGetAllOperation() {
        assertEquals(2, cluster.nodes().size());

        Ignite coordinator = node(0);

        coordinator.sql().executeScript("CREATE ZONE NEW_ZONE (PARTITIONS 2, REPLICAS 1) STORAGE PROFILES ['default']");

        coordinator.sql().executeScript("CREATE TABLE " + TABLE_NAME + " (ID INT PRIMARY KEY, VAL VARCHAR) ZONE NEW_ZONE");

        KeyValueView<Integer, String> kvView = coordinator.tables().table(TABLE_NAME).keyValueView(Integer.class, String.class);

        insertOriginalValues(KEY_COUNT, kvView);

        Transaction roTx = coordinator.transactions().begin(new TransactionOptions().readOnly(true));

        List<Integer> keys = IntStream.range(0, KEY_COUNT).boxed().collect(toList());

        Map<Integer, String> getAllResult = assertDoesNotThrow(() -> kvView.getAll(roTx, keys));

        assertEquals(KEY_COUNT, getAllResult.size());
    }

    private static void insertOriginalValues(int keyCount, KeyValueView<Integer, String> kvView) {
        for (int i = 0; i < keyCount; i++) {
            kvView.put(null, i, "original-" + i);
        }
    }
}
