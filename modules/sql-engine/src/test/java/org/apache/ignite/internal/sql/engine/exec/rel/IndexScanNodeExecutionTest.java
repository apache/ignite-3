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

import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory.Builder;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.index.ColumnCollation;
import org.apache.ignite.internal.index.Index;
import org.apache.ignite.internal.index.IndexDescriptor;
import org.apache.ignite.internal.index.SortedIndex;
import org.apache.ignite.internal.index.SortedIndexDescriptor;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTuplePrefix;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeCondition;
import org.apache.ignite.internal.sql.engine.exec.exp.RangeIterable;
import org.apache.ignite.internal.sql.engine.exec.exp.RexImpTable;
import org.apache.ignite.internal.sql.engine.metadata.PartitionWithTerm;
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Type;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.sql.engine.util.TypeUtils;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Test {@link IndexScanNode} contract.
 * Note: we just bounds are valid and don't care that data meets bound conditions.
 * Bound condition applies in underlying storage, which is mocked here.
 */
@SuppressWarnings("ThrowableNotThrown")
public class IndexScanNodeExecutionTest extends AbstractExecutionTest {
    private static final Comparator<Object[]> comp = Comparator.comparingLong(v -> (long) ((Object[]) v)[0]);

    private static Object[][] sortedIndexData;
    private static Object[][] hashIndexData;

    private static Object[][] sortedScanResult;
    private static Object[][] hashScanResult;

    @BeforeAll
    public static void generateData() {
        sortedIndexData = generateIndexData(2, Commons.IN_BUFFER_SIZE * 2, true);
        hashIndexData = generateIndexData(2, Commons.IN_BUFFER_SIZE * 2, false);
        sortedScanResult = Arrays.stream(sortedIndexData).map(Object[]::clone).sorted(comp).toArray(Object[][]::new);
        hashScanResult = hashIndexData;
    }

    @Test
    public void sortedIndexScanOverEmptyIndex() {
        validateSortedIndexScan(
                EMPTY,
                null,
                null,
                EMPTY
        );
    }

    @Test
    public void sortedIndexScanWithNoBounds() {
        validateSortedIndexScan(
                sortedIndexData,
                null,
                null,
                sortedScanResult
        );
    }

    @Test
    public void sortedIndexScanWithExactBound() {
        // Lower bound.
        validateSortedIndexScan(
                sortedIndexData,
                new Object[]{2L, 1},
                null,
                sortedScanResult
        );
        validateSortedIndexScan(
                sortedIndexData,
                new Object[]{2L, null},
                null,
                sortedScanResult
        );
        validateSortedIndexScan(
                sortedIndexData,
                new Object[]{null, 1},
                null,
                sortedScanResult
        );
        validateSortedIndexScan(
                sortedIndexData,
                new Object[]{null, null},
                null,
                sortedScanResult
        );
        // Upper bound.
        validateSortedIndexScan(
                sortedIndexData,
                null,
                new Object[]{4L, 0},
                sortedScanResult
        );
        validateSortedIndexScan(
                sortedIndexData,
                null,
                new Object[]{4L, null},
                sortedScanResult
        );
        validateSortedIndexScan(
                sortedIndexData,
                null,
                new Object[]{null, 0},
                sortedScanResult
        );
        validateSortedIndexScan(
                sortedIndexData,
                null,
                new Object[]{null, null},
                sortedScanResult
        );
    }

    @Test
    public void sortedIndexScanWithPrefixBound() {
        validateSortedIndexScan(
                sortedIndexData,
                new Object[]{2L, RexImpTable.UNSPECIFIED_VALUE_PLACEHOLDER},
                null,
                sortedScanResult
        );
        validateSortedIndexScan(
                sortedIndexData,
                new Object[]{null, RexImpTable.UNSPECIFIED_VALUE_PLACEHOLDER},
                null,
                sortedScanResult
        );

        validateSortedIndexScan(
                sortedIndexData,
                null,
                new Object[]{4L, RexImpTable.UNSPECIFIED_VALUE_PLACEHOLDER},
                sortedScanResult
        );
        validateSortedIndexScan(
                sortedIndexData,
                null,
                new Object[]{null, RexImpTable.UNSPECIFIED_VALUE_PLACEHOLDER},
                sortedScanResult
        );
    }

    @Test
    public void sortedIndexScanInvalidBounds() {
        assertThrowsWithCause(() ->
                validateSortedIndexScan(
                        sortedIndexData,
                        new Object[]{2L, "Brutus"},
                        null,
                        EMPTY
                ), ClassCastException.class, "class java.lang.String cannot be cast to class java.lang.Integer");

        assertThrowsWithCause(() ->
                validateSortedIndexScan(
                        sortedIndexData,
                        null,
                        new Object[]{3.9, 0},
                        EMPTY
                ), ClassCastException.class, "class java.lang.Double cannot be cast to class java.lang.Long");

        assertThrowsWithCause(() ->
                validateSortedIndexScan(
                        sortedIndexData,
                        new Object[]{1L},
                        null,
                        EMPTY
                ), AssertionError.class, "Invalid range condition");
    }

    @Test
    public void hashIndexLookupOverEmptyIndex() {
        validateHashIndexScan(
                EMPTY,
                new Object[]{1L, 3},
                EMPTY
        );
    }

    @Test
    public void hashIndexLookupNoKey() {
        assertThrowsWithCause(() ->
                validateHashIndexScan(
                        hashIndexData,
                        null,
                        hashScanResult
                ), AssertionError.class, "Invalid hash index condition.");
    }

    @Test
    public void hashIndexLookup() {
        validateHashIndexScan(
                hashIndexData,
                new Object[]{4L, 2},
                hashScanResult);

        validateHashIndexScan(
                hashIndexData,
                new Object[]{null, null},
                hashScanResult);
    }

    @Test
    public void hashIndexLookupInvalidKey() {
        // Hash index doesn't support range scans with prefix bounds.
        assertThrowsWithCause(() ->
                validateHashIndexScan(
                        hashIndexData,
                        new Object[]{2L},
                        EMPTY
                ), AssertionError.class, "Invalid lookup key");

        assertThrowsWithCause(() ->
                validateHashIndexScan(
                        hashIndexData,
                        new Object[]{2L, "Brutus"},
                        EMPTY
                ), ClassCastException.class, "class java.lang.String cannot be cast to class java.lang.Integer");

        assertThrowsWithCause(() ->
                validateHashIndexScan(
                        sortedIndexData,
                        new Object[]{1L, RexImpTable.UNSPECIFIED_VALUE_PLACEHOLDER},
                        EMPTY
                ), AssertionError.class, "Invalid lookup key");
    }

    private static Object[][] generateIndexData(int partCnt, int partSize, boolean sorted) {
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
                long bound2 = ThreadLocalRandom.current().nextLong(3);

                data[rowNum][0] = uniqueNumList.get(rowNum);
                data[rowNum][1] = bound1 == 0 ? null : bound1;
                data[rowNum][2] = bound2 == 0 ? null : bound2;
                data[rowNum][3] = "row-" + rowNum;
            }
        }

        return data;
    }

    private void validateHashIndexScan(Object[][] tableData, @Nullable Object @Nullable [] key, Object[][] expRes) {
        SchemaDescriptor schemaDescriptor = new SchemaDescriptor(
                1,
                new Column[]{new Column("key", NativeTypes.INT64, false)},
                new Column[]{
                        new Column("idxCol1", NativeTypes.INT32, true),
                        new Column("idxCol2", NativeTypes.INT64, true),
                        new Column("val", NativeTypes.stringOf(Integer.MAX_VALUE), true)
                }
        );

        IndexDescriptor indexDescriptor = new IndexDescriptor("IDX1", List.of("idxCol2", "idxCol1"));

        Index<IndexDescriptor> hashIndexMock = mock(Index.class);

        Mockito.doReturn(indexDescriptor).when(hashIndexMock).descriptor();
        //CHECKSTYLE:OFF:Indentation
        Mockito.doAnswer(invocation -> {
                    if (key != null) {
                        validateBound(indexDescriptor, schemaDescriptor, invocation.getArgument(3));
                    }

                    return dummyPublisher(partitionData(tableData, schemaDescriptor, invocation.getArgument(0)));
                })
                .when(hashIndexMock)
                .lookup(Mockito.anyInt(), any(HybridTimestamp.class), any(), any(), any());
        //CHECKSTYLE:ON:Indentation

        IgniteIndex indexMock = mock(IgniteIndex.class);
        Mockito.doReturn(IgniteIndex.Type.HASH).when(indexMock).type();
        Mockito.doReturn(hashIndexMock).when(indexMock).index();
        Mockito.doReturn(indexDescriptor.columns()).when(indexMock).columns();

        validateIndexScan(schemaDescriptor, indexMock, key, key, expRes);
    }

    private void validateSortedIndexScan(
            Object[][] tableData,
            Object @Nullable [] lowerBound,
            Object @Nullable [] upperBound,
            Object[][] expectedData
    ) {
        SchemaDescriptor schemaDescriptor = new SchemaDescriptor(
                1,
                new Column[]{new Column("key", NativeTypes.INT64, false)},
                new Column[]{
                        new Column("idxCol1", NativeTypes.INT32, true),
                        new Column("idxCol2", NativeTypes.INT64, true),
                        new Column("val", NativeTypes.stringOf(Integer.MAX_VALUE), true)
                }
        );

        SortedIndexDescriptor indexDescriptor = new SortedIndexDescriptor(
                "IDX1",
                List.of("idxCol2", "idxCol1"),
                List.of(ColumnCollation.ASC_NULLS_LAST, ColumnCollation.ASC_NULLS_LAST)

        );

        SortedIndex sortedIndexMock = mock(SortedIndex.class);

        //CHECKSTYLE:OFF:Indentation
        Mockito.doAnswer(invocation -> {
                    if (lowerBound != null) {
                        validateBoundPrefix(indexDescriptor, schemaDescriptor, invocation.getArgument(3));
                    }
                    if (upperBound != null) {
                        validateBoundPrefix(indexDescriptor, schemaDescriptor, invocation.getArgument(4));
                    }

                    return dummyPublisher(partitionData(tableData, schemaDescriptor, invocation.getArgument(0)));
                }).when(sortedIndexMock)
                .scan(Mockito.anyInt(), any(HybridTimestamp.class), any(), any(), any(), Mockito.anyInt(), any());
        //CHECKSTYLE:ON:Indentation

        IgniteIndex indexMock = mock(IgniteIndex.class);
        Mockito.doReturn(Type.SORTED).when(indexMock).type();
        Mockito.doReturn(sortedIndexMock).when(indexMock).index();
        Mockito.doReturn(indexDescriptor.columns()).when(indexMock).columns();

        validateIndexScan(schemaDescriptor, indexMock, lowerBound, upperBound, expectedData);
    }

    private void validateIndexScan(
            SchemaDescriptor schemaDescriptor,
            IgniteIndex index,
            Object @Nullable [] lowerBound,
            Object @Nullable [] upperBound,
            Object[][] expectedData
    ) {
        ExecutionContext<Object[]> ectx = executionContext(true);

        RelDataType rowType = createRowTypeFromSchema(ectx.getTypeFactory(), schemaDescriptor);

        RangeIterable<Object[]> rangeIterable = null;

        if (lowerBound != null || upperBound != null) {
            RangeCondition<Object[]> range = mock(RangeCondition.class);

            when(range.lower()).thenReturn(lowerBound);
            when(range.upper()).thenReturn(upperBound);
            when(range.lowerInclude()).thenReturn(true);
            when(range.upperInclude()).thenReturn(true);

            rangeIterable = mock(RangeIterable.class);

            Iterator mockIterator = mock(Iterator.class);
            doCallRealMethod().when(rangeIterable).forEach(any(Consumer.class));
            when(rangeIterable.iterator()).thenReturn(mockIterator);
            when(mockIterator.hasNext()).thenReturn(true, false);
            when(mockIterator.next()).thenReturn(range);
            when(rangeIterable.size()).thenReturn(1);
        }

        IndexScanNode<Object[]> scanNode = new IndexScanNode<>(
                ectx,
                ectx.rowHandler().factory(ectx.getTypeFactory(), rowType),
                index,
                new TestTable(rowType, schemaDescriptor),
                List.of(new PartitionWithTerm(0, -1L), new PartitionWithTerm(2, -1L)),
                index.type() == Type.SORTED ? comp : null,
                rangeIterable,
                null,
                null,
                null
        );

        RootNode<Object[]> node = new RootNode<>(ectx);
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
                .mapToObj(schemaDescriptor::column)
                .forEach(col -> rowTypeBuilder.add(col.name(), TypeUtils.native2relationalType(typeFactory, col.type(), col.nullable())));

        return rowTypeBuilder.build();
    }

    private BinaryRow[] partitionData(Object[][] tableData, SchemaDescriptor schemaDescriptor, int partition) {
        switch (partition) {
            case 0: {

                return Arrays.stream(tableData).limit(tableData.length / 2)
                        .map(r -> convertToRow(schemaDescriptor, r))
                        .toArray(BinaryRow[]::new);
            }
            case 2: {
                return Arrays.stream(tableData).skip(tableData.length / 2)
                        .map(r -> convertToRow(schemaDescriptor, r))
                        .toArray(BinaryRow[]::new);
            }
            default: {
                throw new AssertionError("Undefined partition");
            }
        }
    }

    private BinaryRow convertToRow(SchemaDescriptor schemaDescriptor, Object[] row) {
        RowAssembler asm = new RowAssembler(schemaDescriptor);

        for (int i = 0; i < row.length; i++) {
            RowAssembler.writeValue(asm, schemaDescriptor.column(i), row[i]);
        }

        return asm.build();
    }

    private static Publisher<BinaryRow> dummyPublisher(BinaryRow[] rows) {
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

    private void validateBoundPrefix(IndexDescriptor indexDescriptor, SchemaDescriptor schemaDescriptor, BinaryTuplePrefix boundPrefix) {
        List<String> idxCols = indexDescriptor.columns();

        assertThat(boundPrefix.count(), Matchers.lessThanOrEqualTo(idxCols.size()));

        for (int i = 0; i < boundPrefix.elementCount(); i++) {
            Column col = schemaDescriptor.column(idxCols.get(i));

            if (boundPrefix.hasNullValue(i)) {
                continue;
            }

            Object val = col.type().spec().objectValue(boundPrefix, i);

            assertThat("Column type doesn't match: columnName=" + idxCols.get(i), NativeTypes.fromObject(val), equalTo(col.type()));
        }
    }

    private void validateBound(IndexDescriptor indexDescriptor, SchemaDescriptor schemaDescriptor, BinaryTuple bound) {
        List<String> idxCols = indexDescriptor.columns();

        assertThat(bound.count(), Matchers.equalTo(idxCols.size()));

        for (int i = 0; i < bound.count(); i++) {
            Column col = schemaDescriptor.column(idxCols.get(i));
            Object val = bound.hasNullValue(i) ? null : bound.value(i);

            if (val == null) {
                assertThat("Unexpected null value: columnName" + idxCols.get(i), col.nullable(), Matchers.is(Boolean.TRUE));

                continue;
            }

            assertThat("Column type doesn't match: columnName=" + idxCols.get(i), NativeTypes.fromObject(val), equalTo(col.type()));
        }
    }

    private static class TestTable extends AbstractPlannerTest.TestTable {

        private final SchemaDescriptor schemaDesc;

        public TestTable(RelDataType rowType, SchemaDescriptor schemaDescriptor) {
            super(rowType);

            schemaDesc = schemaDescriptor;
        }

        @Override
        public IgniteDistribution distribution() {
            return IgniteDistributions.broadcast();
        }

        @Override
        public <RowT> RowT toRow(ExecutionContext<RowT> ectx, BinaryRow binaryRow, RowFactory<RowT> factory,
                @Nullable BitSet requiredColumns) {
            TableDescriptor desc = descriptor();
            Row tableRow = new Row(schemaDesc, binaryRow);

            RowT row = factory.create();
            RowHandler<RowT> handler = factory.handler();

            for (int i = 0; i < desc.columnsCount(); i++) {
                handler.set(i, row, TypeUtils.toInternal(tableRow.value(desc.columnDescriptor(i).physicalIndex())));
            }

            return row;
        }
    }
}
