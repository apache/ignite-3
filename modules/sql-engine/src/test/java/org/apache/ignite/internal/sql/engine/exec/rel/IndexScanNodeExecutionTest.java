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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscription;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.index.SortedIndex;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

//TODO: check merge multiple tries.

/**
 * Test {@link IndexScanNode} contract.
 */
public class IndexScanNodeExecutionTest extends AbstractExecutionTest {
    @Test
    public void sortedIndex() {
        // Empty index.
        verifyIndexScan(
                EMPTY,
                ImmutableBitSet.of(0, 1),
                EMPTY
        );

        verifyIndexScan(
                new Object[][]{
                        {1, "Roman", null},
                        {2, "Igor", 4L},
                        {3, "Taras", 3L},
                        {4, "Alexey", 1L},
                        {5, "Ivan", 4L},
                        {6, "Andrey", 2L}
                },
                ImmutableBitSet.of(0, 2),
                // TODO: sort data
                new Object[][]{
                        {1, "Roman", null},
                        {2, "Igor", null},
                        {3, "Taras", null},
                        {4, "Alexey", null},
                        {5, "Ivan", null},
                        {6, "Andrey", null}
                }
        );
    }

    private void verifyIndexScan(Object[][] tableData, ImmutableBitSet requiredColumns, Object[][] expRes) {
        BinaryTuple[] tableRows = createRows(tableData);

        BinaryTuple[] part0Rows = Arrays.stream(tableRows).limit(tableRows.length / 2).toArray(BinaryTuple[]::new);
        BinaryTuple[] part2Rows = Arrays.stream(tableRows).skip(tableRows.length / 2).toArray(BinaryTuple[]::new);

        ExecutionContext<Object[]> ectx = executionContext(true);

        RelDataType rowType = TypeUtils.createRowType(ectx.getTypeFactory(), long.class, String.class, int.class);

        IgniteIndex indexMock = Mockito.mock(IgniteIndex.class);
        SortedIndex sortedIndexMock = Mockito.mock(SortedIndex.class);

        Mockito.doReturn(sortedIndexMock).when(indexMock).index();

        Mockito.doReturn(dummyPublisher(part0Rows)).when(sortedIndexMock)
                .scan(Mockito.eq(0), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyInt(), Mockito.any());
        Mockito.doReturn(dummyPublisher(part2Rows)).when(sortedIndexMock)
                .scan(Mockito.eq(2), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyInt(), Mockito.any());

        AbstractPlannerTest.TestTable table = new AbstractPlannerTest.TestTable(rowType) {
            @Override
            public IgniteDistribution distribution() {
                return IgniteDistributions.any();
            }
        };

        IndexScanNode<Object[]> scanNode = new IndexScanNode<>(
                ectx,
                rowType,
                indexMock,
                table,
                new int[]{0, 2},
                null,
                null,
                null,
                null,
                requiredColumns.toBitSet()
        );

        RootNode<Object[]> node = new RootNode<>(ectx, rowType);
        node.register(scanNode);

        ArrayList<Object[]> res = new ArrayList<>();

        while (node.hasNext()) {
            res.add(node.next());
        }

        assertThat(res.toArray(EMPTY), equalTo(expRes));
    }

    private BinaryTuple[] createRows(Object[][] tableData) {
        BinaryTupleSchema binaryTupleSchema = BinaryTupleSchema.createRowSchema(new SchemaDescriptor(
                1,
                new Column[]{new Column("key", NativeTypes.INT32, false)},
                new Column[]{
                        new Column("idxVal", NativeTypes.INT64, true),
                        new Column("val", NativeTypes.stringOf(Integer.MAX_VALUE), true)
                }
        ));

        return Arrays.stream(tableData)
                .map(row -> new BinaryTuple(binaryTupleSchema, new BinaryTupleBuilder(3, true)
                        .appendInt((int) row[0])
                        .appendLong((Long) row[2])
                        .appendString((String) row[1])
                        .build())
                )
                .toArray(BinaryTuple[]::new);
    }

    @NotNull
    private static Publisher<BinaryTuple> dummyPublisher(BinaryTuple[] rows) {
        return s -> {
            s.onSubscribe(new Subscription() {
                @Override
                public void request(long n) {
                    // No-op.
                }

                @Override
                public void cancel() {
                    // No-op.
                }
            });

            for (int i = 0; i < rows.length; ++i) {
                s.onNext(rows[i]);
            }

            s.onComplete();
        };
    }

    //TODO: Add hash index.
}
