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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.network.DefaultMessagingService;
import org.apache.ignite.internal.table.TxAbstractTest;
import org.apache.ignite.internal.tx.message.TxCleanupMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

/**
 * Durable cleanup test with successful recovery after the failures.
 */
public class ItTxDistributedCleanupRecoveryTest extends TxAbstractTest {

    private AtomicInteger defaultRetryCount;

    /**
     * The constructor.
     *
     * @param testInfo Test info.
     */
    public ItTxDistributedCleanupRecoveryTest(TestInfo testInfo) {
        super(testInfo);
    }

    private void setDefaultRetryCount(int count) {
        defaultRetryCount = new AtomicInteger(count);
    }

    @BeforeEach
    @Override
    public void before() throws Exception {
        // The value of 3 is less than the allowed number of cleanup retries.
        setDefaultRetryCount(3);

        txTestCluster = new ItTxTestCluster(
                testInfo,
                raftConfiguration,
                txConfiguration,
                storageUpdateConfiguration,
                workDir,
                nodes(),
                replicas(),
                startClient(),
                timestampTracker,
                replicationConfiguration
        );

        txTestCluster.prepareCluster();

        this.igniteTransactions = txTestCluster.igniteTransactions;

        accounts = txTestCluster.startTable(ACC_TABLE_NAME, ACCOUNTS_SCHEMA);
        customers = txTestCluster.startTable(CUST_TABLE_NAME, CUSTOMERS_SCHEMA);

        txTestCluster.cluster.forEach(clusterService -> {
            DefaultMessagingService messagingService = (DefaultMessagingService) clusterService.messagingService();
            messagingService.dropMessages((s, networkMessage) -> {
                if (networkMessage instanceof TxCleanupMessage && defaultRetryCount.getAndDecrement() > 0) {
                    logger().info("Dropping cleanup request: {}", networkMessage);

                    return true;
                }
                return false;
            });
        });

        log.info("Tables have been started");
    }

    @Override
    protected int nodes() {
        return 1;
    }
}
