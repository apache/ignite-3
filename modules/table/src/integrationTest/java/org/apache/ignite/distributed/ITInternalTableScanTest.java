/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.RaftServerImpl;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.storage.DataRow;
import org.apache.ignite.internal.storage.Storage;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.basic.SimpleDataRow;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.distributed.raft.PartitionListener;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteUuidGenerator;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterServiceFactory;
import org.apache.ignite.network.MessageSerializationRegistryImpl;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.network.scalecube.TestScaleCubeClusterServiceFactory;
import org.apache.ignite.network.serialization.MessageSerializationRegistry;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupServiceImpl;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link InternalTable#scan(int, org.apache.ignite.tx.Transaction)}
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class ITInternalTableScanTest {
    /** */
    private static final ClusterServiceFactory NETWORK_FACTORY = new TestScaleCubeClusterServiceFactory();

    /** */
    private static final MessageSerializationRegistry SERIALIZATION_REGISTRY = new MessageSerializationRegistryImpl();

    /** */
    private static final RaftMessagesFactory FACTORY = new RaftMessagesFactory();

    /** */
    private static final String TEST_TABLE_NAME = "testTbl";

    /** Mock partition storage. */
    @Mock(lenient = true)
    private Storage mockStorage;

    /** */
    private ClusterService network;

    /** */
    private RaftServer raftSrv;

    /** Internal table to test. */
    private InternalTable internalTbl;

    /**
     * Prepare test environment:
     * <ol>
     *     <li>Start network node.</li>
     *     <li>Start raft server.</li>
     *     <li>Prepare partitioned raft group.</li>
     *     <li>Prepare partitioned raft group service.</li>
     *     <li>Prepare internal table as a test object.</li>
     * </ol>
     *
     * @throws Exception If any.
     */
    @BeforeEach
    public void setUp() throws Exception {
        NetworkAddress nodeNetworkAddress = new NetworkAddress("localhost", 20_000);

        network = ClusterServiceTestUtils.clusterService(
            "Node" + 20_000,
            20_000,
            new StaticNodeFinder(List.of(nodeNetworkAddress)),
            SERIALIZATION_REGISTRY,
            NETWORK_FACTORY
        );

        network.start();

        raftSrv = new RaftServerImpl(network, FACTORY);

        raftSrv.start();

        String grpName = "test_part_grp";

        List<Peer> conf = List.of(new Peer(nodeNetworkAddress));

        mockStorage = mock(Storage.class);

        raftSrv.startRaftGroup(
            grpName,
            new PartitionListener(mockStorage),
            conf
        );

        RaftGroupService raftGrpSvc = RaftGroupServiceImpl.start(
            grpName,
            network,
            FACTORY,
            10_000,
            conf,
            true,
            200
        ).get(3, TimeUnit.SECONDS);

        internalTbl = new InternalTableImpl(
            TEST_TABLE_NAME,
            new IgniteUuidGenerator(UUID.randomUUID(), 0).randomUuid(),
            Map.of(0, raftGrpSvc),
            1
        );
    }

    /**
     * Cleanup previously started network and raft server.
     *
     * @throws Exception If failed to stop component.
     */
    @AfterEach
    public void tearDown() throws Exception {
        if (raftSrv != null)
            raftSrv.beforeNodeStop();

        if (network != null)
            network.beforeNodeStop();

        if (raftSrv != null)
            raftSrv.stop();

        if (network != null)
            network.stop();
    }

    /**
     * Checks whether publisher provides all existing data and then completes if requested by one row at a time.
     */
    @Test
    public void testOneRowScan() throws Exception {
        List<DataRow> subscribedItems = List.of(
            prepareDataRow("key1", "val1"),
            prepareDataRow("key2", "val2")
        );

        AtomicInteger cursorTouchCnt = new AtomicInteger(0);

        List<BinaryRow> retrievedItems = Collections.synchronizedList(new ArrayList<>());

        when(mockStorage.scan(any())).thenAnswer(invocation -> {
            var cursor = mock(Cursor.class);

            when(cursor.hasNext()).thenAnswer(hnInvocation -> cursorTouchCnt.get() < subscribedItems.size());

            when(cursor.next()).thenAnswer(nInvocation -> subscribedItems.get(cursorTouchCnt.getAndIncrement()));

            return cursor;
        });

        AtomicBoolean noMoreData = new AtomicBoolean(false);

        internalTbl.scan(0, null).subscribe(new Subscriber<>() {
            private Subscription subscription;

            @Override public void onSubscribe(Subscription subscription) {
                this.subscription = subscription;

                subscription.request(1);
            }

            @Override public void onNext(BinaryRow item) {
                retrievedItems.add(item);

                subscription.request(1);
            }

            @Override public void onError(Throwable throwable) {
                fail("onError call is not expected.");
            }

            @Override public void onComplete() {
                noMoreData.set(true);
                // TODO: sanpwc Do we really need to call subscription.cancel?
                subscription.cancel();
            }
        });

        assertTrue(waitForCondition(() -> retrievedItems.size() == 2, 1_000));

        List<byte[]> expItems = subscribedItems.stream().map(DataRow::valueBytes).collect(Collectors.toList());
        List<byte[]> gotItems = retrievedItems.stream().map(BinaryRow::bytes).collect(Collectors.toList());

        for (int i = 0; i < expItems.size(); i++)
            assertTrue(Arrays.equals(expItems.get(i), gotItems.get(i)));

        assertTrue(noMoreData.get(), "More data is not expected.");
    }

    /**
     * Checks whether publisher provides all existing data and then completes if requested by multiple rows at a time.
     */
    @Test
    public void testMultipleRowScan() throws Exception {
        List<DataRow> subscribedItems = List.of(
            prepareDataRow("key1", "val1"),
            prepareDataRow("key2", "val2"),
            prepareDataRow("key3", "val3"),
            prepareDataRow("key4", "val4")
        );

        AtomicInteger cursorTouchCnt = new AtomicInteger(0);

        List<BinaryRow> retrievedItems = Collections.synchronizedList(new ArrayList<>());

        when(mockStorage.scan(any())).thenAnswer(invocation -> {
            var cursor = mock(Cursor.class);

            when(cursor.hasNext()).thenAnswer(hnInvocation -> cursorTouchCnt.get() < subscribedItems.size());

            when(cursor.next()).thenAnswer(nInvocation -> subscribedItems.get(cursorTouchCnt.getAndIncrement()));

            return cursor;
        });

        AtomicBoolean noMoreData = new AtomicBoolean(false);

        internalTbl.scan(0, null).subscribe(new Subscriber<>() {
            private Subscription subscription;

            @Override public void onSubscribe(Subscription subscription) {
                this.subscription = subscription;

                subscription.request(1);
            }

            @Override public void onNext(BinaryRow item) {
                retrievedItems.add(item);

                subscription.request(2);
            }

            @Override public void onError(Throwable throwable) {
                fail("onError call is not expected.");
            }

            @Override public void onComplete() {
                noMoreData.set(true);
                // TODO: sanpwc Do we really need to call subscription.cancel?
                subscription.cancel();
            }
        });

        assertTrue(waitForCondition(() -> retrievedItems.size() == 4, 1_000));

        List<byte[]> expItems = subscribedItems.stream().map(DataRow::valueBytes).collect(Collectors.toList());
        List<byte[]> gotItems = retrievedItems.stream().map(BinaryRow::bytes).collect(Collectors.toList());

        for (int i = 0; i < expItems.size(); i++)
            assertTrue(Arrays.equals(expItems.get(i), gotItems.get(i)));

        assertTrue(noMoreData.get(), "More data is not expected.");
    }

    /**
     * Checks that exception from storage properly propagates to subscriber.
     */
    @Test
    public void testExceptionRowScan() throws Exception {
        AtomicReference<Throwable> gotException = new AtomicReference<>();

        when(mockStorage.scan(any())).thenAnswer(invocation -> {
            var cursor = mock(Cursor.class);

            when(cursor.hasNext()).thenAnswer(hnInvocation -> {throw new StorageException("test");});

            return cursor;
        });

        internalTbl.scan(0, null).subscribe(new Subscriber<>() {
            private Subscription subscription;

            @Override public void onSubscribe(Subscription subscription) {
                this.subscription = subscription;

                subscription.request(1);
            }

            @Override public void onNext(BinaryRow item) {
                subscription.request(1);
            }

            @Override public void onError(Throwable throwable) {
                gotException.set(throwable);
                // TODO: sanpwc Do we really need to call subscription.cancel?
                subscription.cancel();
            }

            @Override public void onComplete() {
                // No-op.
            }
        });

        Thread.sleep(1_000);

        assertTrue(waitForCondition(() -> gotException.get() != null, 1_000));
    }

    /**
     * Helper method to convert key and value to {@link DataRow}.
     *
     * @param entryKey Key.
     * @param entryVal Value
     * @return {@link DataRow} based on given key and value.
     *
     * @throws java.io.IOException If failed to close output stream that was used to convertation.
     */
    @SuppressWarnings("ConstantConditions") // It's ok for test.
    private static @NotNull DataRow prepareDataRow(@NotNull String entryKey, @NotNull String entryVal) throws IOException {
        byte[] keyBytes = ByteUtils.toBytes(entryKey);

        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            outputStream.write(keyBytes);
            outputStream.write(ByteUtils.toBytes(entryVal));

            return new SimpleDataRow(keyBytes, outputStream.toByteArray());
        }
    }
}
