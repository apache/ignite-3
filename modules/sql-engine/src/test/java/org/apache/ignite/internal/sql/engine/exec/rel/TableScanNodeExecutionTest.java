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

package org.apache.ignite.internal.sql.engine.exec.rel;

import static org.apache.ignite.internal.replicator.ReplicatorConstants.DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS;
import static org.apache.ignite.internal.sql.engine.util.TypeUtils.rowSchemaFromRelTypes;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.BitSet;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.lowwatermark.TestLowWatermark;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.ClusterNodeImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.SingleClusterNodeResolver;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.TestPlacementDriver;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.PartitionProvider;
import org.apache.ignite.internal.sql.engine.exec.PartitionWithConsistencyToken;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.ScannableTableImpl;
import org.apache.ignite.internal.sql.engine.exec.TableRowConverter;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.sql.engine.framework.ArrayRowHandler;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.table.StreamerReceiverRunner;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.tx.HybridTimestampTracker;
import org.apache.ignite.internal.tx.TxManager;
import org.apache.ignite.internal.tx.configuration.TransactionConfiguration;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.RemotelyTriggeredResourceRegistry;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;
import org.apache.ignite.internal.tx.impl.TransactionInflights;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.internal.tx.test.TestLocalRwTxCounter;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests execution flow of TableScanNode.
 */
@ExtendWith(ConfigurationExtension.class)
public class TableScanNodeExecutionTest extends AbstractExecutionTest<Object[]> {
    private final LinkedList<AutoCloseable> closeables = new LinkedList<>();

    @InjectConfiguration
    private TransactionConfiguration txConfiguration;

    // Ensures that all data from TableScanNode is being propagated correctly.
    @Test
    public void testScanNodeDataPropagation() throws InterruptedException {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();

        RelDataType rowType = TypeUtils.createRowType(tf, TypeUtils.native2relationalTypes(tf,
                NativeTypes.INT32, NativeTypes.STRING, NativeTypes.INT32));

        RowSchema rowSchema = rowSchemaFromRelTypes(List.of(rowType));

        int inBufSize = Commons.IN_BUFFER_SIZE;

        List<PartitionWithConsistencyToken> partsWithConsistencyTokens = IntStream.range(0, TestInternalTableImpl.PART_CNT)
                .mapToObj(p -> new PartitionWithConsistencyToken(p, -1L))
                .collect(Collectors.toList());

        int probingCnt = 50;

        int[] sizes = new int[probingCnt];

        for (int i = 0; i < probingCnt; ++i) {
            sizes[i] = inBufSize * (i + 1) + ThreadLocalRandom.current().nextInt(100);
        }

        RowFactory<Object[]> rowFactory = ctx.rowHandler().factory(rowSchema);

        int i = 0;

        HybridTimestampTracker timestampTracker = new HybridTimestampTracker();

        String leaseholder = "local";

        TopologyService topologyService = mock(TopologyService.class);
        when(topologyService.localMember()).thenReturn(
                new ClusterNodeImpl(leaseholder, leaseholder, NetworkAddress.from("127.0.0.1:1111"))
        );

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.messagingService()).thenReturn(mock(MessagingService.class));
        when(clusterService.topologyService()).thenReturn(topologyService);

        for (int size : sizes) {
            log.info("Check: size=" + size);

            ReplicaService replicaSvc = mock(ReplicaService.class, RETURNS_DEEP_STUBS);

            RemotelyTriggeredResourceRegistry resourcesRegistry = new RemotelyTriggeredResourceRegistry();

            PlacementDriver placementDriver = new TestPlacementDriver(leaseholder, leaseholder);

            HybridClock clock = new HybridClockImpl();
            ClockService clockService = new TestClockService(clock);

            TransactionInflights transactionInflights = new TransactionInflights(placementDriver, clockService);

            TxManagerImpl txManager = new TxManagerImpl(
                    txConfiguration,
                    clusterService,
                    replicaSvc,
                    new HeapLockManager(),
                    clockService,
                    new TransactionIdGenerator(0xdeadbeef),
                    placementDriver,
                    () -> DEFAULT_IDLE_SAFE_TIME_PROPAGATION_PERIOD_MILLISECONDS,
                    new TestLocalRwTxCounter(),
                    resourcesRegistry,
                    transactionInflights,
                    new TestLowWatermark()
            );

            assertThat(txManager.startAsync(new ComponentContext()), willCompleteSuccessfully());

            closeables.add(() -> assertThat(txManager.stopAsync(new ComponentContext()), willCompleteSuccessfully()));

            TestInternalTableImpl internalTable = new TestInternalTableImpl(replicaSvc, size, timestampTracker, txManager);

            TableRowConverter rowConverter = new TableRowConverter() {
                @Override
                public <RowT> BinaryRowEx toFullRow(ExecutionContext<RowT> ectx, RowT row) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public <RowT> BinaryRowEx toKeyRow(ExecutionContext<RowT> ectx, RowT row) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public <RowT> RowT toRow(ExecutionContext<RowT> ectx, BinaryRow tableRow, RowFactory<RowT> factory) {
                    return (RowT) TestInternalTableImpl.ROW;
                }
            };
            ScannableTableImpl scanableTable = new ScannableTableImpl(internalTable, rf -> rowConverter);
            PartitionProvider<Object[]> partitionProvider = PartitionProvider.fromPartitions(partsWithConsistencyTokens);
            TableScanNode<Object[]> scanNode = new TableScanNode<>(ctx, rowFactory, scanableTable,
                    partitionProvider, null, null, null);

            RootNode<Object[]> root = new RootNode<>(ctx);

            root.register(scanNode);

            int cnt = 0;

            while (root.hasNext()) {
                root.next();
                ++cnt;
            }

            internalTable.scanComplete.await();
            assertEquals(sizes[i++] * partsWithConsistencyTokens.size(), cnt);
        }
    }

    @AfterEach
    public void afterEach() throws Exception {
        closeAll(closeables);
    }

    private static class TestInternalTableImpl extends InternalTableImpl {

        private static final Object[] ROW = {1, "2", 3};

        private static final int PART_CNT = 3;

        private final int[] processedPerPart;

        private final int dataAmount;

        private final BinaryRow bbRow = mock(BinaryRow.class);

        private final CopyOnWriteArraySet<Integer> partitions = new CopyOnWriteArraySet<>();

        private final CountDownLatch scanComplete = new CountDownLatch(1);

        TestInternalTableImpl(ReplicaService replicaSvc, int dataAmount, HybridTimestampTracker timestampTracker, TxManager txManager) {
            super(
                    "test",
                    1,
                    PART_CNT,
                    new SingleClusterNodeResolver(mock(ClusterNode.class)),
                    txManager,
                    mock(MvTableStorage.class),
                    mock(TxStateTableStorage.class),
                    replicaSvc,
                    mock(HybridClock.class),
                    timestampTracker,
                    mock(PlacementDriver.class),
                    mock(TransactionInflights.class),
                    3_000,
                    0,
                    null,
                    mock(StreamerReceiverRunner.class)
            );
            this.dataAmount = dataAmount;

            processedPerPart = new int[PART_CNT];
        }

        @Override
        public Publisher<BinaryRow> scan(
                int partId,
                UUID txId,
                HybridTimestamp readTime,
                ClusterNode recipient,
                @Nullable Integer indexId,
                @Nullable BinaryTuplePrefix lowerBound,
                @Nullable BinaryTuplePrefix upperBound,
                int flags,
                @Nullable BitSet columnsToInclude,
                String txCoordinatorId
        ) {
            return s -> {
                s.onSubscribe(new Subscription() {
                    @Override
                    public void request(long n) {
                        int fillAmount = Math.min(dataAmount - processedPerPart[partId], (int) n);

                        processedPerPart[partId] += fillAmount;

                        for (int i = 0; i < fillAmount; ++i) {
                            s.onNext(bbRow);
                        }

                        if (processedPerPart[partId] == dataAmount) {
                            if (partitions.add(partId)) {
                                s.onComplete();

                                if (partitions.size() == PART_CNT) {
                                    scanComplete.countDown();
                                }
                            }
                        }
                    }

                    @Override
                    public void cancel() {
                        // No-op.
                    }
                });
            };
        }
    }

    @Override
    protected RowHandler<Object[]> rowHandler() {
        return ArrayRowHandler.INSTANCE;
    }
}
