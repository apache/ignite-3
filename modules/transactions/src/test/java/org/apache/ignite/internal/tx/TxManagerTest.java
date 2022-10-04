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


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;

import java.util.UUID;
import org.apache.ignite.hlc.HybridClock;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.tx.TransactionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Basic tests for a transaction manager.
 */
@ExtendWith(MockitoExtension.class)
public class TxManagerTest extends IgniteAbstractTest {
    private static final NetworkAddress ADDR = new NetworkAddress("127.0.0.1", 2004);

    private TxManager txManager;

    @Mock
    private ClusterService clusterService;

    @Mock
    private ReplicaService replicaService;

    /** Init test callback. */
    @BeforeEach
    public void before() {
        clusterService = Mockito.mock(ClusterService.class, RETURNS_DEEP_STUBS);

        Mockito.when(clusterService.topologyService().localMember().address()).thenReturn(ADDR);

        replicaService = Mockito.mock(ReplicaService.class, RETURNS_DEEP_STUBS);

        txManager = new TxManagerImpl(replicaService, new HeapLockManager(), new HybridClock());
    }

    @Test
    public void testBegin() throws TransactionException {
        InternalTransaction tx = txManager.begin();

        assertNotNull(tx.id());
        assertEquals(TxState.PENDING, txManager.begin().state());
    }

    @Test
    public void testEnlist() throws TransactionException {
        NetworkAddress addr = clusterService.topologyService().localMember().address();

        assertEquals(ADDR, addr);

        InternalTransaction tx = txManager.begin();

        String replicationGroupName = "ReplicationGroupName";

        ClusterNode node  = Mockito.mock(ClusterNode.class);

        tx.enlist(replicationGroupName, new IgniteBiTuple<>(node, 1L));

        assertEquals(new IgniteBiTuple<>(node, 1L), tx.enlistedNodeAndTerm(replicationGroupName));
    }

    @Test
    public void testId() throws InterruptedException {
        UUID txId1 = Timestamp.nextVersion().toUuid();
        UUID txId2 = Timestamp.nextVersion().toUuid();
        UUID txId3 = Timestamp.nextVersion().toUuid();

        Thread.sleep(1);

        UUID txId4 = Timestamp.nextVersion().toUuid();

        assertTrue(txId2.compareTo(txId1) > 0);
        assertTrue(txId3.compareTo(txId2) > 0);
        assertTrue(txId4.compareTo(txId3) > 0);
    }
}
