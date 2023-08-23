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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.placementdriver.TestPlacementDriver;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.ScannableTableImpl;
import org.apache.ignite.internal.sql.engine.exec.TableRowConverter;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.sql.engine.metadata.PartitionWithTerm;
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest.TestTableDescriptor;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.TransactionIdGenerator;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Tests execution flow of TableScanNode.
 */
public class TableScanNodeExecutionTest extends AbstractExecutionTest {

    // Ensures that all data from TableScanNode is being propagated correctly.
    @Test
    public void testScanNodeDataPropagation() throws InterruptedException {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class, String.class, int.class);

        RowSchema rowSchema = RowSchema.builder()
                .addField(NativeTypes.INT32)
                .addField(NativeTypes.STRING)
                .addField(NativeTypes.INT32)
                .build();

        int inBufSize = Commons.IN_BUFFER_SIZE;

        List<PartitionWithTerm> partsWithTerms = IntStream.range(0, TestInternalTableImpl.PART_CNT)
                .mapToObj(p -> new PartitionWithTerm(p, -1L))
                .collect(Collectors.toList());

        int probingCnt = 50;

        int[] sizes = new int[probingCnt];

        for (int i = 0; i < probingCnt; ++i) {
            sizes[i] = inBufSize * (i + 1) + ThreadLocalRandom.current().nextInt(100);
        }

        RowFactory<Object[]> rowFactory = ctx.rowHandler().factory(rowSchema);

        int i = 0;

        for (int size : sizes) {
            log.info("Check: size=" + size);

            TestInternalTableImpl internalTable = new TestInternalTableImpl(mock(ReplicaService.class), size);

            TableRowConverter rowConverter = new TableRowConverter() {
                @Override
                public <RowT> RowT toRow(ExecutionContext<RowT> ectx, BinaryRow row, RowFactory<RowT> factory,
                        @Nullable BitSet requiredColumns) {
                    return (RowT) TestInternalTableImpl.ROW;
                }
            };
            TableDescriptor descriptor = new TestTableDescriptor(IgniteDistributions::single, rowType);
            ScannableTableImpl scanableTable = new ScannableTableImpl(internalTable, rowConverter, descriptor);
            TableScanNode<Object[]> scanNode = new TableScanNode<>(ctx, rowFactory, scanableTable,
                    partsWithTerms, null, null, null);

            RootNode<Object[]> root = new RootNode<>(ctx);

            root.register(scanNode);

            int cnt = 0;

            while (root.hasNext()) {
                root.next();
                ++cnt;
            }

            internalTable.scanComplete.await();
            assertEquals(sizes[i++] * partsWithTerms.size(), cnt);
        }
    }

    private static class TestInternalTableImpl extends InternalTableImpl {

        private static final Object[] ROW = {1, "2", 3};

        private static final int PART_CNT = 3;

        private final int[] processedPerPart;

        private final int dataAmount;

        private final BinaryRow bbRow = mock(BinaryRow.class);

        private final CopyOnWriteArraySet<Integer> partitions = new CopyOnWriteArraySet<>();

        private final CountDownLatch scanComplete = new CountDownLatch(1);

        TestInternalTableImpl(ReplicaService replicaSvc, int dataAmount) {
            super(
                    "test",
                    1,
                    Int2ObjectMaps.singleton(0, mock(RaftGroupService.class)),
                    PART_CNT,
                    addr -> mock(ClusterNode.class),
                    new TxManagerImpl(replicaSvc, new HeapLockManager(), new HybridClockImpl(), new TransactionIdGenerator(0xdeadbeef)),
                    mock(MvTableStorage.class),
                    mock(TxStateTableStorage.class),
                    replicaSvc,
                    mock(HybridClock.class),
                    new TestPlacementDriver()
            );
            this.dataAmount = dataAmount;

            processedPerPart = new int[PART_CNT];
        }

        @Override
        public Publisher<BinaryRow> scan(
                int partId,
                HybridTimestamp readTime,
                ClusterNode recipient,
                @Nullable Integer indexId,
                @Nullable BinaryTuplePrefix lowerBound,
                @Nullable BinaryTuplePrefix upperBound,
                int flags,
                @Nullable BitSet columnsToInclude
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
}
