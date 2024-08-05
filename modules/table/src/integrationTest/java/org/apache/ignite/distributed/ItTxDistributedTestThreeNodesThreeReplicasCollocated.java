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

package org.apache.ignite.distributed;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.tx.impl.ReadWriteTransactionImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Distributed transaction test using a single partition table, 3 nodes and 3 replicas, collocated on a leader.
 */
public class ItTxDistributedTestThreeNodesThreeReplicasCollocated extends ItTxDistributedTestThreeNodesThreeReplicas {
    /**
     * The constructor.
     *
     * @param testInfo Test info.
     */
    public ItTxDistributedTestThreeNodesThreeReplicasCollocated(TestInfo testInfo) {
        super(testInfo);
    }

    /** {@inheritDoc} */
    @Override protected boolean startClient() {
        return false;
    }

    @Test
    public void testTxStateReplication() throws InterruptedException {
        ReadWriteTransactionImpl tx = (ReadWriteTransactionImpl) igniteTransactions.begin();

        UUID txId = tx.id();

        accounts.recordView().upsert(tx, makeValue(1, 200.));

        tx.commit();

        assertTrue(waitForCondition(
                () -> txTestCluster.txStateStorages.values().stream()
                        .map(txStateStorage -> txStateStorage.get(txId))
                        .filter(txMeta -> txMeta != null && txMeta.txState() == TxState.COMMITTED)
                        .count() >= 2,
                5_000));
    }
}
