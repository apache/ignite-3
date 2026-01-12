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

import static org.apache.ignite.internal.sql.engine.exec.rel.AbstractNode.MODIFY_BATCH_SIZE;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.longs.Long2ObjectMaps;
import it.unimi.dsi.fastutil.longs.LongList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.ignite.internal.sql.engine.api.expressions.RowFactory;
import org.apache.ignite.internal.sql.engine.api.expressions.RowFactory.RowBuilder;
import org.apache.ignite.internal.sql.engine.api.expressions.RowFactoryFactory;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler.RowWrapper;
import org.apache.ignite.internal.sql.engine.exec.TestDownstream;
import org.apache.ignite.internal.sql.engine.exec.UpdatableTable;
import org.apache.ignite.internal.sql.engine.exec.mapping.ColocationGroup;
import org.apache.ignite.internal.sql.engine.exec.mapping.FragmentDescription;
import org.apache.ignite.internal.sql.engine.framework.DataProvider;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptorImpl;
import org.apache.ignite.internal.sql.engine.schema.DefaultValueStrategy;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptorImpl;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.type.NativeTypes.StructTypeBuilder;
import org.apache.ignite.internal.type.StructNativeType;
import org.apache.ignite.internal.type.StructNativeType.Field;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test to verify {@link ModifyNode}.
 */
@SuppressWarnings("resource")
@ExtendWith(MockitoExtension.class)
public class ModifyNodeExecutionTest extends AbstractExecutionTest<RowWrapper> {

    private static final long SOURCE_ID = 42;

    private static final StructNativeType INT_LONG_SCHEMA = NativeTypes.structBuilder()
            .addField("C0", NativeTypes.INT32, true)
            .addField("C1", NativeTypes.INT64, true)
            .build();

    @SuppressWarnings("OverridableMethodCallDuringObjectConstruction")
    private final RowHandler<RowWrapper> handler = rowHandler();

    @Mock
    private UpdatableTable updatableTable;

    @BeforeEach
    void setUpMock() {
        StructNativeType rowSchema = NativeTypes.structBuilder()
                .addField("C1", NativeTypes.INT32, false)
                .addField("C2", NativeTypes.INT64, false)
                .build();

        TableDescriptor tableDescriptor = createTableDescriptor(rowSchema);
        when(updatableTable.descriptor()).thenReturn(tableDescriptor);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, MODIFY_BATCH_SIZE - 1, MODIFY_BATCH_SIZE, MODIFY_BATCH_SIZE + 1, 2 * MODIFY_BATCH_SIZE})
    void nodeReportsExpectedNumberOfUpdatedRowsOnInsert(int sourceSize) {
        ExecutionContext<RowWrapper> context = executionContext();

        Node<RowWrapper> sourceNode = createSource(sourceSize, context);

        ModifyNode<RowWrapper> modifyNode = new ModifyNode<>(
                context, updatableTable, SOURCE_ID, Operation.INSERT, null, INT_LONG_SCHEMA
        );

        TestDownstream<RowWrapper> downstream = new TestDownstream<>();

        modifyNode.register(List.of(sourceNode));
        modifyNode.onRegister(downstream);

        if (sourceSize > 0) {
            when(updatableTable.insertAll(any(), any(), any()))
                    .thenReturn(nullCompletedFuture());
        }

        modifyNode.execute(() -> modifyNode.request(1));

        List<RowWrapper> result = await(downstream.result());

        assertThat(result, notNullValue());
        assertThat(result.get(0), notNullValue());
        assertThat(handler.get(0, result.get(0)), is((long) sourceSize));
        verify(updatableTable, times(numberOfBatches(sourceSize))).insertAll(any(), any(), any());
        verify(updatableTable, times(2)).descriptor();
        verifyNoMoreInteractions(updatableTable);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, MODIFY_BATCH_SIZE - 1, MODIFY_BATCH_SIZE, MODIFY_BATCH_SIZE + 1, 2 * MODIFY_BATCH_SIZE})
    void nodeReportsExpectedNumberOfUpdatedRowsOnUpdate(int sourceSize) {
        ExecutionContext<RowWrapper> context = executionContext();

        Node<RowWrapper> sourceNode = createSource(sourceSize, context);

        ModifyNode<RowWrapper> modifyNode = new ModifyNode<>(
                context, updatableTable, SOURCE_ID, Operation.UPDATE, null, INT_LONG_SCHEMA
        );

        TestDownstream<RowWrapper> downstream = new TestDownstream<>();

        modifyNode.register(List.of(sourceNode));
        modifyNode.onRegister(downstream);

        if (sourceSize > 0) {
            when(updatableTable.upsertAll(any(), any(), any()))
                    .thenReturn(nullCompletedFuture());
        }

        modifyNode.execute(() -> modifyNode.request(1));

        List<RowWrapper> result = await(downstream.result());

        assertThat(result, notNullValue());
        assertThat(result.get(0), notNullValue());
        assertThat(handler.get(0, result.get(0)), is((long) sourceSize));
        verify(updatableTable, times(numberOfBatches(sourceSize))).upsertAll(any(), any(), any());
        verify(updatableTable, times(1)).descriptor();
        verifyNoMoreInteractions(updatableTable);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, MODIFY_BATCH_SIZE - 1, MODIFY_BATCH_SIZE, MODIFY_BATCH_SIZE + 1, 2 * MODIFY_BATCH_SIZE})
    void nodeReportsExpectedNumberOfUpdatedRowsOnDelete(int sourceSize) {
        ExecutionContext<RowWrapper> context = executionContext();

        Node<RowWrapper> sourceNode = createSource(sourceSize, context);

        ModifyNode<RowWrapper> modifyNode = new ModifyNode<>(
                context, updatableTable, SOURCE_ID, Operation.DELETE, null, INT_LONG_SCHEMA
        );

        TestDownstream<RowWrapper> downstream = new TestDownstream<>();

        modifyNode.register(List.of(sourceNode));
        modifyNode.onRegister(downstream);

        if (sourceSize > 0) {
            when(updatableTable.deleteAll(any(), any(), any()))
                    .thenReturn(nullCompletedFuture());
        }

        modifyNode.execute(() -> modifyNode.request(1));

        List<RowWrapper> result = await(downstream.result());

        assertThat(result, notNullValue());
        assertThat(result.get(0), notNullValue());
        assertThat(handler.get(0, result.get(0)), is((long) sourceSize));
        verify(updatableTable, times(numberOfBatches(sourceSize))).deleteAll(any(), any(), any());
        verify(updatableTable, times(1)).descriptor();
        verifyNoMoreInteractions(updatableTable);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, MODIFY_BATCH_SIZE - 1, MODIFY_BATCH_SIZE, MODIFY_BATCH_SIZE + 1, 2 * MODIFY_BATCH_SIZE})
    void exceptionIsPassedThroughToErrorHandlerOnInsert(int sourceSize) {
        ExecutionContext<RowWrapper> context = executionContext();

        Node<RowWrapper> sourceNode = createSource(sourceSize, context);

        ModifyNode<RowWrapper> modifyNode = new ModifyNode<>(
                context, updatableTable, SOURCE_ID, Operation.INSERT, null, INT_LONG_SCHEMA
        );

        TestDownstream<RowWrapper> downstream = new TestDownstream<>();

        modifyNode.register(List.of(sourceNode));
        modifyNode.onRegister(downstream);

        RuntimeException expected = new RuntimeException("this is expected");
        when(updatableTable.insertAll(any(), any(), any()))
                .thenReturn(CompletableFuture.failedFuture(expected));

        modifyNode.execute(() -> modifyNode.request(1));

        assertThat(downstream.result(), willThrow(is(expected)));
        verify(updatableTable).insertAll(any(), any(), any());
        verify(updatableTable, times(2)).descriptor();
        verifyNoMoreInteractions(updatableTable);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, MODIFY_BATCH_SIZE - 1, MODIFY_BATCH_SIZE, MODIFY_BATCH_SIZE + 1, 2 * MODIFY_BATCH_SIZE})
    void exceptionIsPassedThroughToErrorHandlerOnUpdate(int sourceSize) {
        ExecutionContext<RowWrapper> context = executionContext();

        Node<RowWrapper> sourceNode = createSource(sourceSize, context);

        ModifyNode<RowWrapper> modifyNode = new ModifyNode<>(
                context, updatableTable, SOURCE_ID, Operation.UPDATE, null, INT_LONG_SCHEMA
        );

        TestDownstream<RowWrapper> downstream = new TestDownstream<>();

        modifyNode.register(List.of(sourceNode));
        modifyNode.onRegister(downstream);

        RuntimeException expected = new RuntimeException("this is expected");
        when(updatableTable.upsertAll(any(), any(), any()))
                .thenReturn(CompletableFuture.failedFuture(expected));

        modifyNode.execute(() -> modifyNode.request(1));

        assertThat(downstream.result(), willThrow(is(expected)));
        verify(updatableTable).upsertAll(any(), any(), any());
        verify(updatableTable, times(1)).descriptor();
        verifyNoMoreInteractions(updatableTable);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, MODIFY_BATCH_SIZE - 1, MODIFY_BATCH_SIZE, MODIFY_BATCH_SIZE + 1, 2 * MODIFY_BATCH_SIZE})
    void exceptionIsPassedThroughToErrorHandlerOnDelete(int sourceSize) {
        ExecutionContext<RowWrapper> context = executionContext();

        Node<RowWrapper> sourceNode = createSource(sourceSize, context);

        ModifyNode<RowWrapper> modifyNode = new ModifyNode<>(
                context, updatableTable, SOURCE_ID, Operation.DELETE, null, INT_LONG_SCHEMA
        );

        TestDownstream<RowWrapper> downstream = new TestDownstream<>();

        modifyNode.register(List.of(sourceNode));
        modifyNode.onRegister(downstream);

        RuntimeException expected = new RuntimeException("this is expected");
        when(updatableTable.deleteAll(any(), any(), any()))
                .thenReturn(CompletableFuture.failedFuture(expected));

        modifyNode.execute(() -> modifyNode.request(1));

        assertThat(downstream.result(), willThrow(is(expected)));
        verify(updatableTable).deleteAll(any(), any(), any());
        verify(updatableTable, times(1)).descriptor();
        verifyNoMoreInteractions(updatableTable);
    }

    private static Stream<Arguments> mergeArgs() {
        return Stream.of(
                // Column count, update only, destination row data
                Arguments.of(2, false, new Object[]{null, null}),
                Arguments.of(2, true,  new Object[]{null, 1}),
                Arguments.of(2, true,  new Object[]{2, 1}),

                Arguments.of(3, false, new Object[]{null, null, null}),
                Arguments.of(3, true, new Object[]{null, 1, null}),
                Arguments.of(3, true, new Object[]{null, null, 1}),
                Arguments.of(3, true, new Object[]{2, null, null}),

                Arguments.of(4, false, new Object[]{null, null, null, null}),
                Arguments.of(4, true, new Object[]{null, 1, null, null}),
                Arguments.of(4, true, new Object[]{null, null, 1, null}),
                Arguments.of(4, true, new Object[]{null, null, null, 1}),
                Arguments.of(4, true, new Object[]{2, null, null, null}),

                Arguments.of(5, false, new Object[]{null, null, null, null, null}),
                Arguments.of(5, true, new Object[]{null, 1, null, null, null}),
                Arguments.of(5, true, new Object[]{null, null, 1, null, null}),
                Arguments.of(5, true, new Object[]{null, null, null, 1, null}),
                Arguments.of(5, true, new Object[]{null, null, null, null, 1}),
                Arguments.of(5, true, new Object[]{2, null, null, null, null})
        );
    }

    @ParameterizedTest
    @MethodSource("mergeArgs")
    void mergePassesCorrectRowsToInsert(int colCount, boolean updateOnly, Object[] dstRow2Data) {
        // DestinationRow: dst_c1, dst_c2, dst_c3
        // SourceRow: src_c1, src_c2
        // MergeRow:  src_c1, src_c2, null, dst_c1, dst_c2, dst_c3, update_col1, ...,

        ExecutionContext<RowWrapper> context = executionContext();
        RowHandler<RowWrapper> rowHandler = context.rowAccessor();

        StructTypeBuilder dstRowSchemaBuilder = NativeTypes.structBuilder();

        for (int i = 0; i < colCount; i++) {
            dstRowSchemaBuilder.addField("C" + i, NativeTypes.INT32, true);
        }

        StructNativeType dstRowSchema = dstRowSchemaBuilder.build();

        StructTypeBuilder srcRowSchemaBuilder = NativeTypes.structBuilder();

        for (int i = 0; i < colCount; i++) {
            srcRowSchemaBuilder.addField("C" + i, NativeTypes.INT32, true);
        }

        StructNativeType srcRowSchema = srcRowSchemaBuilder.build();

        StructNativeType updateSchema = NativeTypes.structBuilder()
                .addField("C0", NativeTypes.INT32, true)
                .build();

        StructNativeType mergeRowSchema = concat(srcRowSchema, dstRowSchema, updateSchema);

        Mockito.reset(updatableTable);

        TableDescriptor tableDescriptor = createTableDescriptor(dstRowSchema);

        ArgumentCaptor<List<RowWrapper>> insertedRows = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<List<RowWrapper>> updatedRows = ArgumentCaptor.forClass(List.class);

        when(updatableTable.descriptor()).thenReturn(tableDescriptor);

        if (updateOnly) {
            when(updatableTable.upsertAll(any(), updatedRows.capture(), any())).thenReturn(nullCompletedFuture());
        } else {
            when(updatableTable.insertAll(any(), insertedRows.capture(), any())).thenReturn(nullCompletedFuture());
            when(updatableTable.upsertAll(any(), updatedRows.capture(), any())).thenReturn(nullCompletedFuture());
        }

        RowFactory<RowWrapper> dstFactory = context.rowFactoryFactory().create(dstRowSchema);

        Object[] dstRow1Data = new Object[colCount];
        dstRow1Data[0] = 1;
        RowWrapper dstRow1 = dstFactory.create(dstRow1Data);

        RowWrapper dstRow2 = dstFactory.create(dstRow2Data);

        RowFactory<RowWrapper> srcFactory = context.rowFactoryFactory().create(srcRowSchema);

        Object[] srcRow1Data = new Object[colCount];
        srcRow1Data[0] = 2;
        RowWrapper srcRow1 = srcFactory.create(srcRow1Data);

        Object[] srcRow2Data = new Object[colCount];
        srcRow2Data[0] = 1;
        srcRow2Data[1] = 5;
        RowWrapper srcRow2 = srcFactory.create(srcRow2Data);

        RowFactory<RowWrapper> updateFactory = context.rowFactoryFactory().create(updateSchema);

        RowWrapper update = updateFactory.create(4);
        RowWrapper noUpdate = updateFactory.create(new Object[]{null});

        RowFactory<RowWrapper> mergeRowFactory = context.rowFactoryFactory().create(mergeRowSchema);

        RowWrapper mergeRow1 = concatRow(mergeRowFactory, srcRow1, dstRow1, noUpdate);
        RowWrapper mergeRow2 = concatRow(mergeRowFactory, srcRow2, dstRow2, update);

        Node<RowWrapper> sourceNode = new ScanNode<>(
                context, DataProvider.fromCollection(List.of(mergeRow1, mergeRow2))
        );

        TestDownstream<RowWrapper> downstream = new TestDownstream<>();

        ModifyNode<RowWrapper> modifyNode = new ModifyNode<>(
                context, updatableTable, SOURCE_ID, Operation.MERGE, List.of("C1"), mergeRowSchema
        );
        modifyNode.register(List.of(sourceNode));
        modifyNode.onRegister(downstream);

        modifyNode.execute(() -> modifyNode.request(1));

        await(downstream.result());

        if (updateOnly) {
            verify(updatableTable).upsertAll(any(), any(), any());

            RowWrapper updated1 = updatedRows.getAllValues().get(0).get(0);
            expectRow(updated1, rowHandler, colCount, Arrays.asList(1, null));

            RowWrapper updated2 = updatedRows.getAllValues().get(0).get(1);

            List<Object> updated2Expected = new ArrayList<>(Arrays.asList(dstRow2Data[0], 4));
            updated2Expected.addAll(Arrays.asList(dstRow2Data).subList(2, colCount));

            expectRow(updated2, rowHandler, colCount, updated2Expected);
        } else {
            verify(updatableTable).insertAll(any(), any(), any());
            verify(updatableTable).upsertAll(any(), any(), any());

            RowWrapper inserted = insertedRows.getAllValues().get(0).get(0);
            expectRow(inserted, rowHandler, colCount, Arrays.asList(1, 5));

            RowWrapper updated = updatedRows.getAllValues().get(0).get(0);
            expectRow(updated, rowHandler, colCount, Arrays.asList(1, null));
        }
    }

    private RowWrapper concatRow(RowFactory<RowWrapper> rowFactory, RowWrapper...  rows) {
        RowHandler<RowWrapper> handler = rowHandler();
        RowBuilder<RowWrapper> builder = rowFactory.rowBuilder();

        for (RowWrapper row : rows) {
            int cols = handler.columnsCount(row);
            for (int i = 0; i < cols; i++) {
                builder.addField(handler.get(i, row));
            }
        }

        return builder.build();
    }

    private static TableDescriptor createTableDescriptor(StructNativeType rowSchema) {
        List<ColumnDescriptor> columns = new ArrayList<>();

        for (int i = 0; i < rowSchema.fields().size(); i++) {
            Field field = rowSchema.fields().get(i);
            ColumnDescriptorImpl col = new ColumnDescriptorImpl(
                    field.name(),
                    false,
                    false,
                    false,
                    field.nullable(),
                    i,
                    field.type(),
                    DefaultValueStrategy.DEFAULT_NULL,
                    null
            );
            columns.add(col);
        }

        return new TableDescriptorImpl(columns, IgniteDistributions.single());
    }

    private static void expectRow(RowWrapper row, RowHandler<RowWrapper> rowHandler, int expectedRowSize, List<Object> expectRowPrefix) {
        int rowSize = rowHandler.columnsCount(row);

        assertEquals(expectedRowSize, rowSize);
        assertTrue(expectRowPrefix.size() <= rowSize, "Incorrect number of expected vals");

        for (int i = 0; i < rowSize; i++) {
            if (i < expectRowPrefix.size()) {
                assertEquals(expectRowPrefix.get(i), rowHandler.get(i, row), "col#" + i + " " + rowHandler.toString(row));
            } else {
                assertNull(rowHandler.get(i, row), "col#" + i + " " + rowHandler.toString(row));
            }
        }
    }

    private static int numberOfBatches(int rowCount) {
        return rowCount / MODIFY_BATCH_SIZE + (rowCount % MODIFY_BATCH_SIZE == 0 ? 0 : 1);
    }

    private static Node<RowWrapper> createSource(int rowCount, ExecutionContext<RowWrapper> context) {
        return new ScanNode<>(
                context, DataProvider.fromRow(context.rowFactoryFactory().create(INT_LONG_SCHEMA).create(1, 1L), rowCount)
        );
    }

    @Override
    protected RowHandler<RowWrapper> rowHandler() {
        return SqlRowHandler.INSTANCE;
    }

    @Override
    protected RowFactoryFactory<RowWrapper> rowFactoryFactory() {
        return SqlRowHandler.INSTANCE;
    }

    @Override
    protected FragmentDescription getFragmentDescription() {
        ColocationGroup colocationGroup = new ColocationGroup(LongList.of(), List.of(), Int2ObjectMaps.emptyMap());
        return new FragmentDescription(0, true, Long2ObjectMaps.singleton(SOURCE_ID, colocationGroup), null, null, null);
    }

    private static StructNativeType concat(StructNativeType... types) {
        StructTypeBuilder builder = NativeTypes.structBuilder();

        for (StructNativeType type : types) {
            for (Field field : type.fields()) {
                builder.addField(field.name(), field.type(), field.nullable());
            }
        }

        return builder.build();
    }
}
