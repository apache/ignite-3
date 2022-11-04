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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory.Builder;
import org.apache.ignite.internal.binarytuple.BinaryTupleBuilder;
import org.apache.ignite.internal.index.ColumnCollation;
import org.apache.ignite.internal.index.Index;
import org.apache.ignite.internal.index.IndexDescriptor;
import org.apache.ignite.internal.index.SortedIndex;
import org.apache.ignite.internal.index.SortedIndexDescriptor;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeCondition;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeIterable;
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Type;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Test {@link IndexScanNode} contract.
 */
public class IndexScanNodeExecutionTest extends AbstractExecutionTest {

    @Test
    public void sortedIndex() {
        // Empty index.
        validateSortedIndexScan(
                EMPTY,
                null,
                null,
                EMPTY
        );

        Comparator<Object[]> comp = Comparator.comparingLong(v -> (long) ((Object[]) v)[0]);

        Object[][] tableData = generateIndexData(2, Commons.IN_BUFFER_SIZE * 2, true);
        Object[][] expected = Arrays.stream(tableData).map(Object[]::clone).sorted(comp).toArray(Object[][]::new);

        // Validate sort order.
        validateSortedIndexScan(
                tableData,
                null,
                null,
                expected
        );

        // Validate bounds.
        validateSortedIndexScan(
                tableData,
                new Object[]{2, 1},
                new Object[]{3, 0},
                expected
        );

        validateSortedIndexScan(
                tableData,
                new Object[]{2, 1},
                new Object[]{4},
                expected

        );

        validateSortedIndexScan(
                tableData,
                new Object[]{null},
                null,
                expected
        );

        // Validate failure due to incorrect bounds.
        IgniteTestUtils.assertThrowsWithCause(() ->
                validateSortedIndexScan(
                        tableData,
                        new Object[]{2, "Brutus"},
                        new Object[]{3.9, 0},
                        EMPTY
                ), ClassCastException.class);
    }

    @Test
    public void hashIndex() {
        // Empty index.
        validateHashIndexScan(
                EMPTY,
                null,
                EMPTY
        );

        Object[][] tableData = generateIndexData(2, Commons.IN_BUFFER_SIZE * 2, false);

        // Validate data.
        validateHashIndexScan(
                tableData,
                null,
                tableData
        );

        // Validate bounds.
        validateHashIndexScan(
                tableData,
                new Object[]{4, 2},
                tableData);

        validateHashIndexScan(
                tableData,
                new Object[]{null, null},
                tableData);

        // Validate failure due to incorrect bounds.
        IgniteTestUtils.assertThrowsWithCause(() ->
                validateHashIndexScan(
                        tableData,
                        new Object[]{2},
                        EMPTY
                ), AssertionError.class, "Invalid lookup condition");

        IgniteTestUtils.assertThrowsWithCause(() ->
                validateHashIndexScan(
                        tableData,
                        new Object[]{2, "Brutus"},
                        EMPTY
                ), ClassCastException.class);
    }

    private Object[][] generateIndexData(int partCnt, int partSize, boolean sorted) {
        Set<Long> uniqueNumbers = new HashSet<>();

        while (uniqueNumbers.size() < partCnt * partSize) {
            uniqueNumbers.add(ThreadLocalRandom.current().nextLong());
        }

        List<Long> uniqueNumList = new ArrayList<>(uniqueNumbers);

        Object[][] data = new Object[partCnt * partSize][4];

        for (int p = 0; p < partCnt; p++) {
            if (sorted) {
                uniqueNumList.subList(p * partSize, (p + 1) * partSize).sort(Comparator.comparingLong(v -> v));
            }

            for (int j = 0; j < partSize; j++) {
                int rowNum = p * partSize + j;

                data[rowNum] = new Object[4];

                int bound1 = ThreadLocalRandom.current().nextInt(3);
                int bound2 = ThreadLocalRandom.current().nextInt(3);

                data[rowNum][0] = uniqueNumList.get(rowNum);
                data[rowNum][1] = bound1 == 0 ? null : bound1;
                data[rowNum][2] = bound2 == 0 ? null : bound2;;
                data[rowNum][3] = "row-" + rowNum;
            }
        }

        return data;
    }

    private void validateHashIndexScan(Object[][] tableData, @Nullable Object[] key, Object[][] expRes) {
        SchemaDescriptor schemaDescriptor = new SchemaDescriptor(
                1,
                new Column[]{new Column("key", NativeTypes.INT64, false)},
                new Column[]{
                        new Column("idxCol1", NativeTypes.INT32, true),
                        new Column("idxCol2", NativeTypes.INT32, true),
                        new Column("val", NativeTypes.stringOf(Integer.MAX_VALUE), true)
                }
        );

        IgniteIndex indexMock = Mockito.mock(IgniteIndex.class);

        IndexDescriptor indexDescriptor = new IndexDescriptor("IDX1", List.of("idxCol2", "idxCol1"));

        Index<IndexDescriptor> hashIndexMock = Mockito.mock(Index.class);

        Mockito.doReturn(indexDescriptor).when(hashIndexMock).descriptor();
        //CHECKSTYLE:OFF:Indentation
        Mockito.doAnswer(invocation -> {
                    if (key != null) {
                        validateBound(indexDescriptor, schemaDescriptor, invocation.getArgument(2));
                    }

                    return dummyPublisher(partitionData(tableData, schemaDescriptor, invocation.getArgument(0)));
                })
                .when(hashIndexMock)
                .scan(Mockito.anyInt(), Mockito.any(), Mockito.any(), Mockito.any());
        //CHECKSTYLE:ON:Indentation

        Mockito.doReturn(IgniteIndex.Type.HASH).when(indexMock).type();
        Mockito.doReturn(hashIndexMock).when(indexMock).index();

        validateIndexScan(tableData, schemaDescriptor, indexMock, key, key, expRes);
    }

    private void validateSortedIndexScan(
            Object[][] tableData,
            Object[] lowerBound,
            Object[] upperBound,
            Object[][] expectedData
    ) {
        SchemaDescriptor schemaDescriptor = new SchemaDescriptor(
                1,
                new Column[]{new Column("key", NativeTypes.INT64, false)},
                new Column[]{
                        new Column("idxCol1", NativeTypes.INT32, true),
                        new Column("idxCol2", NativeTypes.INT32, true),
                        new Column("val", NativeTypes.stringOf(Integer.MAX_VALUE), true)
                }
        );

        IgniteIndex indexMock = Mockito.mock(IgniteIndex.class);

        SortedIndexDescriptor indexDescriptor = new SortedIndexDescriptor(
                "IDX1",
                List.of("idxCol2", "idxCol1"),
                List.of(ColumnCollation.ASC_NULLS_FIRST, ColumnCollation.ASC_NULLS_LAST)

        );

        SortedIndex sortedIndexMock = Mockito.mock(SortedIndex.class);

        Mockito.doReturn(indexDescriptor).when(sortedIndexMock).descriptor();
        //CHECKSTYLE:OFF:Indentation
        Mockito.doAnswer(invocation -> {
                    if (lowerBound != null) {
                        validateBoundPrefix(indexDescriptor, schemaDescriptor, invocation.getArgument(2));
                    }
                    if (upperBound != null) {
                        validateBoundPrefix(indexDescriptor, schemaDescriptor, invocation.getArgument(3));
                    }

                    return dummyPublisher(partitionData(tableData, schemaDescriptor, invocation.getArgument(0)));
                }).when(sortedIndexMock)
                .scan(Mockito.anyInt(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.anyInt(), Mockito.any());
        //CHECKSTYLE:ON:Indentation

        Mockito.doReturn(Type.SORTED).when(indexMock).type();
        Mockito.doReturn(sortedIndexMock).when(indexMock).index();

        validateIndexScan(tableData, schemaDescriptor, indexMock, lowerBound, upperBound, expectedData);
    }

    private void validateIndexScan(
            Object[][] tableData,
            SchemaDescriptor schemaDescriptor,
            IgniteIndex index,
            Object[] lowerBound,
            Object[] upperBound,
            Object[][] expectedData
    ) {
        ExecutionContext<Object[]> ectx = executionContext(true);

        RelDataType rowType = createRowTypeFromSchema(ectx.getTypeFactory(), schemaDescriptor);

        RangeIterable<Object[]> rangeIterable = null;

        if (lowerBound != null || upperBound != null) {
            RangeCondition<Object[]> range = Mockito.mock(RangeCondition.class);

            Mockito.doReturn(lowerBound).when(range).lower();
            Mockito.doReturn(upperBound).when(range).upper();
            Mockito.doReturn(true).when(range).lowerInclude();
            Mockito.doReturn(true).when(range).upperInclude();

            rangeIterable = Mockito.mock(RangeIterable.class);

            Mockito.doReturn(1).when(rangeIterable).size();
            Mockito.doAnswer(inv -> List.of(range).iterator()).when(rangeIterable).iterator();
        }

        IndexScanNode<Object[]> scanNode = new IndexScanNode<>(
                ectx,
                rowType,
                index,
                new TestTable(rowType),
                new int[]{0, 2},
                index.type() == Type.SORTED ? Comparator.comparingLong(v -> (long) ((Object[]) v)[0]) : null,
                rangeIterable,
                null,
                null,
                null
        );

        RootNode<Object[]> node = new RootNode<>(ectx, scanNode.rowType());
        node.register(scanNode);

        int n = 0;

        while (node.hasNext()) {
            assertThat(node.next(), equalTo(expectedData[n++]));
        }

        assertThat(n, equalTo(expectedData.length));
    }

    private static RelDataType createRowTypeFromSchema(IgniteTypeFactory typeFactory, SchemaDescriptor schemaDescriptor) {
        Builder rowTypeBuilder = new Builder(typeFactory);

        IntStream.range(0, schemaDescriptor.length())
                .mapToObj(i -> schemaDescriptor.column(i))
                .forEach(col -> rowTypeBuilder.add(col.name(), TypeUtils.native2relationalType(typeFactory, col.type(), col.nullable())));

        return rowTypeBuilder.build();
    }

    private BinaryTuple[] partitionData(Object[][] tableData, SchemaDescriptor schemaDescriptor, int partition) {
        BinaryTupleSchema binaryTupleSchema = BinaryTupleSchema.createRowSchema(schemaDescriptor);

        switch (partition) {
            case 0: {
                return Arrays.stream(tableData).limit(tableData.length / 2)
                        .map(r -> convertToTuple(binaryTupleSchema, r))
                        .toArray(BinaryTuple[]::new);
            }
            case 2: {
                return Arrays.stream(tableData).skip(tableData.length / 2)
                        .map(r -> convertToTuple(binaryTupleSchema, r))
                        .toArray(BinaryTuple[]::new);
            }
            default: {
                throw new AssertionError("Undefined partition");
            }
        }
    }

    private BinaryTuple convertToTuple(BinaryTupleSchema binaryTupleSchema, Object[] row) {
        return new BinaryTuple(binaryTupleSchema, new BinaryTupleBuilder(4, true)
                .appendLong((long) row[0])
                .appendInt((Integer) row[1])
                .appendInt((Integer) row[2])
                .appendString((String) row[3])
                .build());
    }

    private static Publisher<BinaryTuple> dummyPublisher(BinaryTuple[] rows) {
        return s -> {
            s.onSubscribe(new Subscription() {
                int off = 0;
                boolean completed = false;

                @Override
                public void request(long n) {
                    int start = off;
                    int end = Math.min(start + (int) n, rows.length);

                    off = end;

                    for (int i = start; i < end; i++) {
                        s.onNext(rows[i]);
                    }

                    if (off >= rows.length && !completed) {
                        completed = true;

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

    private void validateBoundPrefix(IndexDescriptor indexDescriptor, SchemaDescriptor schemaDescriptor, BinaryTuple boundPrefix) {
        List<String> idxCols = indexDescriptor.columns();

        assertThat(boundPrefix.count(), Matchers.lessThanOrEqualTo(idxCols.size()));

        for (int i = 0; i < boundPrefix.count(); i++) {
            Column col = schemaDescriptor.column(idxCols.get(i));
            Object val = boundPrefix.value(i);

            if (col.nullable() && val == null) {
                continue;
            }

            assertThat("Column type doesn't match: columnName=" + idxCols.get(i), NativeTypes.fromObject(val), equalTo(col.type()));
        }
    }

    private void validateBound(IndexDescriptor indexDescriptor, SchemaDescriptor schemaDescriptor, BinaryTuple bound) {
        List<String> idxCols = indexDescriptor.columns();

        assertThat(bound.count(), Matchers.equalTo(idxCols.size()));

        for (int i = 0; i < bound.count(); i++) {
            Column col = schemaDescriptor.column(idxCols.get(i));
            Object val = bound.value(i);

            if (col.nullable() && val == null) {
                continue;
            }

            assertThat("Column type doesn't match: columnName=" + idxCols.get(i), NativeTypes.fromObject(val), equalTo(col.type()));
        }
    }

    private static class TestTable extends AbstractPlannerTest.TestTable {
        public TestTable(RelDataType rowType) {
            super(rowType);
        }

        @Override
        public IgniteDistribution distribution() {
            return IgniteDistributions.broadcast();
        }
    }
}
