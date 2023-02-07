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

import java.util.BitSet;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.metadata.PartitionWithTerm;
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest;
import org.apache.ignite.internal.sql.engine.schema.InternalIgniteTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.distributed.storage.InternalTableImpl;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.TxManagerImpl;
import org.apache.ignite.internal.tx.storage.state.TxStateTableStorage;
import org.apache.ignite.internal.utils.PrimaryReplica;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Tests execution flow of TableScanNode.
 */
public class TableScanNodeExecutionTest extends AbstractExecutionTest {
    private static int dataAmount;

    // Ensures that all data from TableScanNode is being propagated correctly.
    @Test
    public void testScanNodeDataPropagation() {
        ExecutionContext<Object[]> ctx = executionContext();
        IgniteTypeFactory tf = ctx.getTypeFactory();
        RelDataType rowType = TypeUtils.createRowType(tf, int.class, String.class, int.class);

        int inBufSize = Commons.IN_BUFFER_SIZE;

        List<PartitionWithTerm> partsWithTerms = Stream.of(0, 1, 2)
                .map(p -> new PartitionWithTerm(p, -1L))
                .collect(Collectors.toList());

        int probingCnt = 50;

        int[] sizes = new int[probingCnt];

        for (int i = 0; i < probingCnt; ++i) {
            sizes[i] = inBufSize * (i + 1) + ThreadLocalRandom.current().nextInt(100);
        }

        InternalIgniteTable tbl = new TestTable(rowType);

        RowFactory<Object[]> rowFactory = ctx.rowHandler().factory(ctx.getTypeFactory(), rowType);

        int i = 0;

        for (int size : sizes) {
            log.info("Check: size=" + size);

            dataAmount = size;

            TableScanNode<Object[]> scanNode = new TableScanNode<>(ctx, rowFactory, tbl, partsWithTerms, null, null, null);

            RootNode<Object[]> root = new RootNode<>(ctx);

            root.register(scanNode);

            int cnt = 0;

            while (root.hasNext()) {
                root.next();
                ++cnt;
            }

            assertEquals(sizes[i++] * partsWithTerms.size(), cnt);
        }
    }

    private static class TestTable extends AbstractPlannerTest.TestTable {
        private static final Object[] res = {1, "2", 3};

        public TestTable(RelDataType rowType) {
            super(rowType);
        }

        @Override
        public IgniteDistribution distribution() {
            return IgniteDistributions.broadcast();
        }

        @Override
        public InternalTable table() {
            ConcurrentMap<Integer, RaftGroupService> partMap = new ConcurrentSkipListMap<>();

            partMap.put(0, mock(RaftGroupService.class));

            return new TestInternalTableImpl(mock(ReplicaService.class), partMap);
        }

        @Override
        public <RowT> RowT toRow(ExecutionContext<RowT> ectx, BinaryRow row, RowFactory<RowT> factory,
                @Nullable BitSet requiredColumns) {
            return (RowT) res;
        }
    }

    private static class TestInternalTableImpl extends InternalTableImpl {
        private int[] processedPerPart;

        private static final int PART_CNT = 3;

        private final ByteBufferRow bbRow = new ByteBufferRow(new byte[1]);

        public TestInternalTableImpl(
                ReplicaService replicaSvc,
                ConcurrentMap partMap
        ) {
            super(
                    "test",
                    UUID.randomUUID(),
                    partMap,
                    PART_CNT,
                    addr -> mock(ClusterNode.class),
                    new TxManagerImpl(replicaSvc, new HeapLockManager(), new HybridClockImpl()),
                    mock(MvTableStorage.class),
                    mock(TxStateTableStorage.class),
                    replicaSvc,
                    mock(HybridClock.class)
            );

            processedPerPart = new int[PART_CNT];
        }

        @Override
        public Publisher<BinaryRow> scan(
                int partId,
                UUID txId,
                PrimaryReplica recipient,
                @Nullable UUID indexId,
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
                            s.onComplete();
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
