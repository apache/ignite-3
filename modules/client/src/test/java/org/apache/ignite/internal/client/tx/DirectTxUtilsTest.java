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

package org.apache.ignite.internal.client.tx;

import static java.util.UUID.randomUUID;
import static org.apache.ignite.internal.hlc.HybridTimestampTracker.EMPTY_TS_PROVIDER;
import static org.apache.ignite.internal.hlc.HybridTimestampTracker.emptyTracker;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.client.ClientChannel;
import org.apache.ignite.internal.client.ClientClusterNode;
import org.apache.ignite.internal.client.ClientTransactionInflights;
import org.apache.ignite.internal.client.PartitionMapping;
import org.apache.ignite.internal.client.ProtocolContext;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.WriteContext;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Tests for {@link DirectTxUtils}.
 */
@SuppressWarnings("resource")
public class DirectTxUtilsTest extends BaseIgniteAbstractTest {
    @Test
    void resolveChannelReturnsTxChannelForReadOnlyTx() {
        ReliableChannel ch = mock(ReliableChannel.class);
        ClientChannel txChannel = mockClientChannel("node1");

        WriteContext ctx = new WriteContext(emptyTracker(), ClientOp.TUPLE_GET);

        // Read-only tx with mapping pointing to a different node.
        ClientTransaction roTx = createTx(txChannel, ch, true, null);
        PartitionMapping mapping = new PartitionMapping(1, "node2", 0);

        CompletableFuture<ClientChannel> result = DirectTxUtils.resolveChannel(ctx, ch, false, roTx, mapping);

        assertSame(txChannel, result.join());
        verify(ch, never()).getChannelAsync(Mockito.any());
    }

    @Test
    void resolveChannelReturnsTxChannelWhenNoCommitPartition() {
        ReliableChannel ch = mock(ReliableChannel.class);
        ClientChannel txChannel = mockClientChannel("node1");

        // RW tx without commit partition.
        ClientTransaction tx = createTx(txChannel, ch, false, null);

        WriteContext ctx = new WriteContext(emptyTracker(), ClientOp.TUPLE_UPSERT);
        PartitionMapping mapping = new PartitionMapping(1, "node2", 0);

        CompletableFuture<ClientChannel> result = DirectTxUtils.resolveChannel(ctx, ch, true, tx, mapping);

        assertSame(txChannel, result.join());
        verify(ch, never()).getChannelAsync(Mockito.any());
    }

    @Test
    void resolveChannelReturnsTxChannelWhenSameNode() {
        ReliableChannel ch = mock(ReliableChannel.class);
        ClientChannel txChannel = mockClientChannel("node1");

        when(ch.getChannelAsync("node1")).thenReturn(CompletableFuture.completedFuture(txChannel));

        // RW tx with commit partition, mapping points to the same node as the tx coordinator.
        PartitionMapping commitPm = new PartitionMapping(1, "node1", 0);
        ClientTransaction tx = createTx(txChannel, ch, false, commitPm);

        WriteContext ctx = new WriteContext(emptyTracker(), ClientOp.TUPLE_UPSERT);
        PartitionMapping mapping = new PartitionMapping(2, "node1", 0);

        CompletableFuture<ClientChannel> result = DirectTxUtils.resolveChannel(ctx, ch, true, tx, mapping);

        assertSame(txChannel, result.join());
    }

    @Test
    void resolveChannelUsesGetChannelAsyncForDifferentNode() {
        ReliableChannel ch = mock(ReliableChannel.class);
        when(ch.inflights()).thenReturn(new ClientTransactionInflights());

        ClientChannel txChannel = mockClientChannel("node1");
        ClientChannel otherChannel = mockClientChannel("node2");

        when(ch.getChannelAsync("node2")).thenReturn(CompletableFuture.completedFuture(otherChannel));

        // RW tx with commit partition, mapping points to a different node.
        PartitionMapping commitPm = new PartitionMapping(1, "node1", 0);
        ClientTransaction tx = createTx(txChannel, ch, false, commitPm);

        WriteContext ctx = new WriteContext(emptyTracker(), ClientOp.TUPLE_UPSERT);
        PartitionMapping mapping = new PartitionMapping(2, "node2", 0);

        CompletableFuture<ClientChannel> result = DirectTxUtils.resolveChannel(ctx, ch, true, tx, mapping);

        assertSame(otherChannel, result.join());
        verify(ch).getChannelAsync("node2");
    }

    @Test
    void resolveChannelThrowsForDifferentReliableChannel() {
        ReliableChannel ch1 = mock(ReliableChannel.class);
        ReliableChannel ch2 = mock(ReliableChannel.class);
        ClientChannel txChannel = mockClientChannel("node1");

        ClientTransaction tx = createTx(txChannel, ch1, false, null);

        WriteContext ctx = new WriteContext(emptyTracker(), ClientOp.TUPLE_UPSERT);

        assertThrows(IllegalArgumentException.class, () -> DirectTxUtils.resolveChannel(ctx, ch2, true, tx, null));
    }

    @SuppressWarnings("DataFlowIssue")
    private static ClientChannel mockClientChannel(String nodeName) {
        ClientChannel channel = mock(ClientChannel.class);
        ProtocolContext protocolContext = mock(ProtocolContext.class);
        ClientClusterNode clusterNode = new ClientClusterNode(randomUUID(), nodeName, null);

        when(channel.protocolContext()).thenReturn(protocolContext);
        when(protocolContext.clusterNode()).thenReturn(clusterNode);

        return channel;
    }

    private static ClientTransaction createTx(
            ClientChannel channel,
            ReliableChannel reliableChannel,
            boolean readOnly,
            @Nullable PartitionMapping commitPartitionMapping
    ) {
        return new ClientTransaction(
                channel, reliableChannel, 1, readOnly, randomUUID(),
                commitPartitionMapping, randomUUID(), EMPTY_TS_PROVIDER, 0);
    }
}
