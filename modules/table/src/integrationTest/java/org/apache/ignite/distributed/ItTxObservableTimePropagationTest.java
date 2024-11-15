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

import static org.apache.ignite.internal.tx.impl.ResourceVacuumManager.RESOURCE_VACUUM_INTERVAL_MILLISECONDS_PROPERTY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import org.apache.ignite.internal.TestHybridClock;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.table.TxInfrastructureTest;
import org.apache.ignite.internal.testframework.SystemPropertiesExtension;
import org.apache.ignite.internal.testframework.WithSystemProperty;
import org.apache.ignite.internal.tx.TxStateMeta;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests if commit timestamp is propagated to observable time correctly.
 */
@ExtendWith(SystemPropertiesExtension.class)
@WithSystemProperty(key = RESOURCE_VACUUM_INTERVAL_MILLISECONDS_PROPERTY, value = "1000000")
public class ItTxObservableTimePropagationTest extends TxInfrastructureTest {
    private static final long CLIENT_FROZEN_PHYSICAL_TIME = 3000;

    /**
     * The constructor.
     *
     * @param testInfo Test info.
     */
    public ItTxObservableTimePropagationTest(TestInfo testInfo) {
        super(testInfo);
    }

    @Override
    protected int nodes() {
        return 3;
    }

    @Override
    protected int replicas() {
        return 3;
    }

    @Override
    protected HybridClock createClocks(ClusterNode node) {
        // Client physical time is frozen in the past, server time advances normally.
        return new TestHybridClock(() -> node.address().port() == 19999 ? CLIENT_FROZEN_PHYSICAL_TIME : System.currentTimeMillis());
    }

    @Test
    public void testImplicitObservableTimePropagation() {
        RecordView<Tuple> view = accounts.recordView();
        view.upsert(null, makeValue(1, 100.0));
        TxManagerImpl clientTxManager = (TxManagerImpl) txTestCluster.clientTxManager;
        Collection<TxStateMeta> states = clientTxManager.states();
        assertEquals(1, states.size());
        HybridTimestamp commitTs = states.iterator().next().commitTimestamp();
        assertNotNull(commitTs);
        assertEquals(commitTs, timestampTracker.get());
        assertTrue(commitTs.getPhysical() != CLIENT_FROZEN_PHYSICAL_TIME, "Client time should be advanced to server time");
    }
}
