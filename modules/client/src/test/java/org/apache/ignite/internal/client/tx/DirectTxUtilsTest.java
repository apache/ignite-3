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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledHeapByteBuf;
import java.util.HexFormat;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.client.ClientChannel;
import org.apache.ignite.internal.client.ClientClusterNode;
import org.apache.ignite.internal.client.ClientTransactionInflights;
import org.apache.ignite.internal.client.PartitionMapping;
import org.apache.ignite.internal.client.ProtocolContext;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.WriteContext;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.internal.client.proto.ResponseFlags;
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

    @Test
    void decode() {
        decode("6669656c64373a3231313638383532");
        decode("656c64393a2d313138303534333437");
        decode("313034373533313738333138393030");
        decode("3132393137383436383a3135383236");
        decode("303734303731363a31363332353035");
    }

    void decode(String hexStr) {
        byte[] bytes = hexToBytes(hexStr);
        String utfStr = new String(bytes);
        System.out.println(utfStr); // field7:21168852
        ByteBuf buf = Unpooled.wrappedBuffer(bytes);
        ClientMessageUnpacker in = new ClientMessageUnpacker(buf);

        // Header
        Long resId = in.unpackLong();
        int flags = in.unpackInt();

        var error = ResponseFlags.getErrorFlag(flags);
        var notification = ResponseFlags.getNotificationFlag(flags);
        var partitionAssignmentChanged = ResponseFlags.getPartitionAssignmentChangedFlag(flags);

        long maxStartTime = - 1;

        if (partitionAssignmentChanged) {
            maxStartTime = in.unpackLong();
        }

        long observableTimestamp = in.unpackLong();

        // Tx
        long id = in.unpackLong();
        UUID txId = in.unpackUuid();
        UUID coordId = in.unpackUuid();
        long timeout = in.unpackLong();
    }

    @Test
    void decode2() {
        String hexStr = "38323832303934333a38363435303230373a3233373633383133313a323035393231383632373a2d323033353530373738393a2d313738757365723833303133373839313036313434343737373a6669656c64383a2d313534373732323139373a2d313938383430323636333a3337353333373334353a2d313534323733383936333a2d3835363432373100006adfce005e080100d30000000000000000ce002f0439d8031800a69d993a9d0100000000964e01acd8036c4e604f40bc0ab24bcaaf32c12dfd97cdea60011ac3c504160117007b00df004301a7010b026f02d30237039b03ff0375736572343738323030323430343538393738393939367573";
        byte[] bytes = hexToBytes(hexStr);
        String utfStr = new String(bytes);
        System.out.println(utfStr);

        // 82820943:86450207:237638131:2059218627:-2035507789:-178user830137891061444777:field8:-1547722197:-1988402663:375337345:-1542738963:-8564271  j��  �        � /9� ���:�    �N��lN`O@�
        //�Kʯ2�-����`�� { � C�o�7��user4782002404589789996us
    }

    public static byte[] hexToBytes(String hex) {
        int len = hex.length();
        if (len % 2 != 0) {
            throw new IllegalArgumentException("Hex string must have even length");
        }

        byte[] result = new byte[len / 2];

        for (int i = 0; i < len; i += 2) {
            int high = Character.digit(hex.charAt(i), 16);
            int low = Character.digit(hex.charAt(i + 1), 16);

            if (high == -1 || low == -1) {
                throw new IllegalArgumentException("Invalid hex character");
            }

            result[i / 2] = (byte) ((high << 4) + low);
        }

        return result;
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
