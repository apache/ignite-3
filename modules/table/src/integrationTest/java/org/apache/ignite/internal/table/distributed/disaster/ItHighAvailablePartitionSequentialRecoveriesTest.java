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

import org.apache.ignite.internal.app.IgniteImpl;
import org.junit.jupiter.api.Test;

/** Test multiple HA zone partitions recovery in a row. */
public class ItHighAvailablePartitionSequentialRecoveriesTest extends AbstractHighAvailablePartitionsRecoveryTest {
    @Override
    protected int initialNodes() {
        return 5;
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return FAST_FAILURE_DETECTION_NODE_BOOTSTRAP_CFG_TEMPLATE;
    }

    @Test
    void testTwoSequentialResets() throws InterruptedException {
        createHaZoneWithTable();

        IgniteImpl node = igniteImpl(0);

        assertRecoveryKeyIsEmpty(node);

        stopNodes(2, 3, 4);

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, PARTITION_IDS, nodeNames(0, 1));

        stopNode(1);

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, PARTITION_IDS, nodeNames(0));
    }
}
