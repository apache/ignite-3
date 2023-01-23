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

package org.apache.ignite.internal.table.distributed.raft.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.impl.TestMvTableStorage;
import org.apache.ignite.internal.tx.storage.state.TxStateStorage;
import org.apache.ignite.internal.tx.storage.state.test.TestTxStateTableStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * For {@link PartitionAccessImpl} testing.
 */
@ExtendWith(ConfigurationExtension.class)
public class PartitionAccessImplTest {
    private static final int TEST_PARTITION_ID = 0;

    @Test
    void testMinMaxLastAppliedIndex(@InjectConfiguration("mock.tables.foo {}") TablesConfiguration tablesConfig) {
        TableConfiguration tableCfg = tablesConfig.tables().get("foo");

        TestMvTableStorage mvTableStorage = new TestMvTableStorage(tableCfg, tablesConfig);
        TestTxStateTableStorage txStateTableStorage = new TestTxStateTableStorage();

        MvPartitionStorage mvPartitionStorage = mvTableStorage.getOrCreateMvPartition(TEST_PARTITION_ID);
        TxStateStorage txStateStorage = txStateTableStorage.getOrCreateTxStateStorage(TEST_PARTITION_ID);

        PartitionAccess partitionAccess = new PartitionAccessImpl(
                new PartitionKey(UUID.randomUUID(), TEST_PARTITION_ID),
                mvTableStorage,
                txStateTableStorage
        );

        assertEquals(0, partitionAccess.minLastAppliedIndex());
        assertEquals(0, partitionAccess.maxLastAppliedIndex());

        mvPartitionStorage.runConsistently(() -> {
            mvPartitionStorage.lastApplied(10, 1);
            txStateStorage.lastApplied(5, 1);

            return null;
        });

        assertEquals(5, partitionAccess.minLastAppliedIndex());
        assertEquals(10, partitionAccess.maxLastAppliedIndex());

        mvPartitionStorage.runConsistently(() -> {
            mvPartitionStorage.lastApplied(15, 2);

            txStateStorage.lastApplied(20, 2);

            return null;
        });

        assertEquals(15, partitionAccess.minLastAppliedIndex());
        assertEquals(20, partitionAccess.maxLastAppliedIndex());
    }

    @Test
    void testMinMaxLastAppliedTerm(@InjectConfiguration("mock.tables.foo {}") TablesConfiguration tablesConfig) {
        TableConfiguration tableCfg = tablesConfig.tables().get("foo");

        TestMvTableStorage mvTableStorage = new TestMvTableStorage(tableCfg, tablesConfig);
        TestTxStateTableStorage txStateTableStorage = new TestTxStateTableStorage();

        MvPartitionStorage mvPartitionStorage = mvTableStorage.getOrCreateMvPartition(TEST_PARTITION_ID);
        TxStateStorage txStateStorage = txStateTableStorage.getOrCreateTxStateStorage(TEST_PARTITION_ID);

        PartitionAccess partitionAccess = new PartitionAccessImpl(
                new PartitionKey(UUID.randomUUID(), TEST_PARTITION_ID),
                mvTableStorage,
                txStateTableStorage
        );

        assertEquals(0, partitionAccess.minLastAppliedTerm());
        assertEquals(0, partitionAccess.maxLastAppliedTerm());

        mvPartitionStorage.runConsistently(() -> {
            mvPartitionStorage.lastApplied(1, 10);
            txStateStorage.lastApplied(1, 5);

            return null;
        });

        assertEquals(5, partitionAccess.minLastAppliedTerm());
        assertEquals(10, partitionAccess.maxLastAppliedTerm());

        mvPartitionStorage.runConsistently(() -> {
            mvPartitionStorage.lastApplied(2, 15);

            txStateStorage.lastApplied(2, 20);

            return null;
        });

        assertEquals(15, partitionAccess.minLastAppliedTerm());
        assertEquals(20, partitionAccess.maxLastAppliedTerm());
    }
}
