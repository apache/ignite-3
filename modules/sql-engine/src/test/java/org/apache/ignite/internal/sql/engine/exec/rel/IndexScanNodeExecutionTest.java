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
import java.util.List;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscription;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory.Builder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.index.ColumnCollation;
import org.apache.ignite.internal.index.SortedIndex;
import org.apache.ignite.internal.index.SortedIndexDescriptor;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.InternalIgniteTable;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

//TODO: check merging of multiple sorted index tries is correct.
//TODO: add test for hash index.

/**
 * Test {@link IndexScanNode} contract.
 */
public class IndexScanNodeExecutionTest extends AbstractExecutionTest {
    @Test
    public void sortedIndex() {
        // Empty index.
        verifyIndexScan(
                EMPTY,
                ImmutableBitSet.of(1),
                EMPTY
        );

        verifyIndexScan(
                new Object[][]{
                        {1L, null, 1, "Roman"},
                        {2L, 4, 2, "Igor"},
                        {3L, 3, 1, "Taras"},
                        {4L, 2, null, "Alexey"},
                        {5L, 4, 1, "Ivan"},
                        {6L, 2, 1, "Andrey"}
                },
                ImmutableBitSet.of(2),
                // TODO: sort data, once IndexScanNode will support merging.
                new Object[][]{
                        {1L, null, 1, "Roman"},
                        {2L, 4, 2, "Igor"},
                        {3L, 3, 1, "Taras"},
                        {4L, 2, null, "Alexey"},
                        {5L, 4, 1, "Ivan"},
                        {6L, 2, 1, "Andrey"}
                }
        );
    }

    private void verifyIndexScan(Object[][] tableData, ImmutableBitSet requiredColumns, Object[][] expRes) {
        SchemaDescriptor schemaDescriptor = new SchemaDescriptor(
                1,
                new Column[]{new Column("key", NativeTypes.INT64, false)},
                new Column[]{
                        new Column("idxCol1", NativeTypes.INT32, true),
                        new Column("idxCol2", NativeTypes.INT32, true),
                        new Column("val", NativeTypes.stringOf(Integer.MAX_VALUE), true)
                }
        );
        BinaryTuple[] tableRows = convertToRows(tableData, schemaDescriptor);

        BinaryTuple[] part0Rows = Arrays.stream(tableRows).limit(tableRows.length / 2).toArray(BinaryTuple[]::new);
        BinaryTuple[] part2Rows = Arrays.stream(tableRows).skip(tableRows.length / 2).toArray(BinaryTuple[]::new);

        ExecutionContext<Object[]> ectx = executionContext(true);

        IgniteIndex indexMock = Mockito.mock(IgniteIndex.class);
        SortedIndex sortedIndexMock = Mockito.mock(SortedIndex.class);

        Mockito.doReturn(sortedIndexMock).when(indexMock).index();
        Mockito.doReturn(new SortedIndexDescriptor(
                        "IDX1",
                        List.of("idxCol2", "idxCol1"),
                        List.of(ColumnCollation.ASC_NULLS_FIRST, ColumnCollation.ASC_NULLS_LAST)
                ))
                .when(sortedIndexMock).descriptor();

        Mockito.doReturn(dummyPublisher(part0Rows)).when(sortedIndexMock)
                .scan(Mockito.eq(0), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyInt(), Mockito.any());
        Mockito.doReturn(dummyPublisher(part2Rows)).when(sortedIndexMock)
                .scan(Mockito.eq(2), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyInt(), Mockito.any());

        IgniteTypeFactory f = ectx.getTypeFactory();

        RelDataType rowType = new Builder(f)
                .add("key", TypeUtils.native2relationalType(f, NativeTypes.INT64, false))
                .add("idxCol1", TypeUtils.native2relationalType(f, NativeTypes.INT32, true))
                .add("idxCol2", TypeUtils.native2relationalType(f, NativeTypes.INT32, true))
                .add("val", TypeUtils.native2relationalType(f, NativeTypes.STRING, true))
                .build();
        InternalIgniteTable table = new AbstractPlannerTest.TestTable(rowType) {
            @Override
            public IgniteDistribution distribution() {
                return IgniteDistributions.broadcast();
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

    private BinaryTuple[] convertToRows(Object[][] tableData, SchemaDescriptor schemaDescriptor) {
        BinaryTupleSchema binaryTupleSchema = BinaryTupleSchema.createRowSchema(schemaDescriptor);

        return Arrays.stream(tableData)
                .map(row -> new BinaryTuple(binaryTupleSchema, new BinaryTupleBuilder(4, true)
                        .appendLong((long) row[0])
                        .appendInt((Integer) row[1])
                        .appendInt((Integer) row[2])
                        .appendString((String) row[3])
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
}
