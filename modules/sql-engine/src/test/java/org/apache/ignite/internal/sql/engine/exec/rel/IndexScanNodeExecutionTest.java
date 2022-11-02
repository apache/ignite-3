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
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Flow.Publisher;
import java.util.concurrent.Flow.Subscription;
import java.util.stream.IntStream;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory.Builder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
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
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex.Type;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistribution;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
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

    // TODO IGNITE-17813: sort data, once IndexScanNode will support merging.
    public static final Object[][] TABLE_DATA = new Object[][]{
            {1L, null, 1, "Roman"},
            {2L, 4, 2, "Igor"},
            {3L, 3, 1, "Taras"},
            {4L, 2, null, "Alexey"},
            {5L, 4, 1, "Ivan"},
            {6L, null, null, "Andrey"}
    };

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
                TABLE_DATA,
                null,
                null,
                TABLE_DATA
        );
    }

    @Test
    public void sortedIndexScan() {
        validateSortedIndexScan(
                TABLE_DATA,
                new Object[]{null, 2, 1, null},
                new Object[]{null, 3, 0, null},
                TABLE_DATA
        );
    }

    @Test
    public void sortedIndexScan2() {
        validateSortedIndexScan(
                TABLE_DATA,
                new Object[]{null, 2, 1, null},
                new Object[]{null, 4, null, null},
                TABLE_DATA

        );
        validateSortedIndexScan(
                TABLE_DATA,
                new Object[]{null, 2, null, null},
                new Object[]{null, 4, 0, null},
                TABLE_DATA

        );
    }

    @Test
    public void sortedIndexScanNoUpperBound() {
        validateSortedIndexScan(
                TABLE_DATA,
                new Object[]{null, 2, 1, null},
                null,
                TABLE_DATA
        );

        validateSortedIndexScan(
                TABLE_DATA,
                new Object[]{null, null, null, null},
                null,
                TABLE_DATA
        );
    }

    @Test
    public void sortedIndexScanNoLowerBound() {
        validateSortedIndexScan(
                TABLE_DATA,
                null,
                new Object[]{null, 4, 0, null},
                TABLE_DATA
        );

        validateSortedIndexScan(
                TABLE_DATA,
                null,
                new Object[]{null, null, null, null},
                TABLE_DATA
        );
    }


    @Test
    public void sortedIndexScanInvalidBounds() {
        IgniteTestUtils.assertThrowsWithCause(() ->
                validateSortedIndexScan(
                        TABLE_DATA,
                        new Object[]{null, 2, "Brutus", null},
                        new Object[]{null, 3.9, 0, null},
                        EMPTY
                ), ClassCastException.class, "class java.lang.String cannot be cast to class java.lang.Integer");

        IgniteTestUtils.assertThrowsWithCause(() ->
                validateSortedIndexScan(
                        TABLE_DATA,
                        new Object[]{null, 2},
                        new Object[]{null, 3},
                        EMPTY
                ), ArrayIndexOutOfBoundsException.class, "Index 2 out of bounds for length 2");
    }

    @Test
    public void hashIndexLookupOverEmptyIndex() {
        validateHashIndexScan(
                EMPTY,
                new Object[]{null, 1, 3, null},
                EMPTY
        );
    }

    @Test
    public void hashIndexLookupNoKey() {
        // Validate data.
        validateHashIndexScan(
                TABLE_DATA,
                null,
                TABLE_DATA
        );
    }

    @Test
    public void hashIndexLookup() {
        validateHashIndexScan(
                TABLE_DATA,
                new Object[]{null, 4, 2, null},
                TABLE_DATA);
    }

    @Test
    public void hashIndexLookupEmptyKey() {
        validateHashIndexScan(
                TABLE_DATA,
                new Object[]{null, null, null, null},
                TABLE_DATA);
    }

    @Test
    public void hashIndexLookupInvalidKey() {
        IgniteTestUtils.assertThrowsWithCause(() ->
                validateHashIndexScan(
                        TABLE_DATA,
                        new Object[]{2},
                        EMPTY
                ), ArrayIndexOutOfBoundsException.class, "Index 2 out of bounds for length 1");

        IgniteTestUtils.assertThrowsWithCause(() ->
                validateHashIndexScan(
                        TABLE_DATA,
                        new Object[]{null, 2, "Brutus", null},
                        EMPTY
                ), ClassCastException.class, "class java.lang.String cannot be cast to class java.lang.Integer");
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

        IgniteIndex indexMock = Mockito.mock(IgniteIndex.class);
        Mockito.doReturn(IgniteIndex.Type.HASH).when(indexMock).type();
        Mockito.doReturn(hashIndexMock).when(indexMock).index();
        Mockito.doReturn(indexDescriptor.columns()).when(indexMock).columns();

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

        SortedIndexDescriptor indexDescriptor = new SortedIndexDescriptor(
                "IDX1",
                List.of("idxCol2", "idxCol1"),
                List.of(ColumnCollation.ASC_NULLS_FIRST, ColumnCollation.ASC_NULLS_LAST)

        );

        SortedIndex sortedIndexMock = Mockito.mock(SortedIndex.class);

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

        IgniteIndex indexMock = Mockito.mock(IgniteIndex.class);
        Mockito.doReturn(Type.SORTED).when(indexMock).type();
        Mockito.doReturn(sortedIndexMock).when(indexMock).index();
        Mockito.doReturn(indexDescriptor.columns()).when(indexMock).columns();

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

            when(range.lower()).thenReturn(lowerBound);
            when(range.upper()).thenReturn(upperBound);
            when(range.lowerInclude()).thenReturn(true);
            when(range.upperInclude()).thenReturn(true);

            rangeIterable = Mockito.mock(RangeIterable.class);

            when(rangeIterable.size()).thenReturn(1);
            when(rangeIterable.iterator()).thenAnswer(inv -> List.of(range).iterator());
        }

        ImmutableIntList idxColMapping = ImmutableIntList.of(index.columns().stream()
                .map(schemaDescriptor::column)
                .mapToInt(Column::schemaIndex)
                .toArray());

        IndexScanNode<Object[]> scanNode = new IndexScanNode<>(
                ectx,
                rowType,
                index,
                new TestTable(rowType, schemaDescriptor),
                idxColMapping,
                new int[]{0, 2},
                rangeIterable,
                null,
                null,
                null
        );

        RootNode<Object[]> node = new RootNode<>(ectx, scanNode.rowType());
        node.register(scanNode);

        ArrayList<Object[]> res = new ArrayList<>();

        while (node.hasNext()) {
            res.add(node.next());
        }

        assertThat(res.toArray(EMPTY), equalTo(expectedData));
    }

    private static RelDataType createRowTypeFromSchema(IgniteTypeFactory typeFactory, SchemaDescriptor schemaDescriptor) {
        Builder rowTypeBuilder = new Builder(typeFactory);

        IntStream.range(0, schemaDescriptor.length())
                .mapToObj(i -> schemaDescriptor.column(i))
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
        RowAssembler asm = new RowAssembler(schemaDescriptor, 0, 1);

        for (int i = 0; i < row.length; i++) {
            RowAssembler.writeValue(asm, schemaDescriptor.column(i), row[i]);
        }

        return asm.build();
    }

    private static Publisher<BinaryRow> dummyPublisher(BinaryRow[] rows) {
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
            Object val = bound.value(i);

            if (col.nullable() && val == null) {
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
                @Nullable ImmutableBitSet requiredColumns) {
            TableDescriptor desc = descriptor();
            Row tableRow = new Row(schemaDesc, binaryRow);

            RowT row = factory.create();
            RowHandler<RowT> handler = factory.handler();

            for (int i = 0; i < desc.columnsCount(); i++) {
                handler.set(i, row, TypeUtils.toInternal(ectx, tableRow.value(desc.columnDescriptor(i).physicalIndex())));
            }

            return row;
        }
    }
}
