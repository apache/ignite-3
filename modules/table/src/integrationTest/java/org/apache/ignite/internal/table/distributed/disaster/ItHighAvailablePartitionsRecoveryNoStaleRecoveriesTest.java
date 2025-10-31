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

package org.apache.ignite.internal.table.distributed.disaster;

import static org.apache.ignite.internal.ConfigTemplates.FAST_FAILURE_DETECTION_NODE_BOOTSTRAP_CFG_TEMPLATE;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.apache.ignite.internal.app.IgniteImpl;
import org.junit.jupiter.api.Test;

/** Test that only one actor write the {@link DisasterRecoveryRequest} to the metastore. */
public class ItHighAvailablePartitionsRecoveryNoStaleRecoveriesTest extends AbstractHighAvailablePartitionsRecoveryTest {
    @Override
    protected int initialNodes() {
        return 4;
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return FAST_FAILURE_DETECTION_NODE_BOOTSTRAP_CFG_TEMPLATE;
    }

    @Test
    void testHaRecoveryWhenMajorityLossNoRedundantResetRequests() throws InterruptedException {
        createHaZoneWithTable();

        IgniteImpl node0 = igniteImpl(0);
        IgniteImpl node1 = igniteImpl(1);

        assertRecoveryKeyIsEmpty(node0);

        String lastStoppedNode = node(3).name();

        stopNodes(2, 3);

        long updateRevision = waitForSpecificZoneTopologyReductionAndReturnUpdateRevision(node0, HA_ZONE_NAME, Set.of(lastStoppedNode));

        assertTrue(waitForCondition(() -> node0.metaStorageManager().appliedRevision() >= updateRevision, 10_000));
        assertTrue(waitForCondition(() -> node1.metaStorageManager().appliedRevision() >= updateRevision, 10_000));

        assertTrue(waitForCondition(() -> node0.disasterRecoveryManager().ongoingOperationsById().isEmpty(), 10_000));
        assertTrue(waitForCondition(() -> node1.disasterRecoveryManager().ongoingOperationsById().isEmpty(), 10_000));

        assertRecoveryRequestWasOnlyOne(node0);
        assertRecoveryRequestWasOnlyOne(node1);
    }
}
