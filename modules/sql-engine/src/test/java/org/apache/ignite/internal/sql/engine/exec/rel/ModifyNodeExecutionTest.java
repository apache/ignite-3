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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.RowHandler.RowFactory;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler.RowWrapper;
import org.apache.ignite.internal.sql.engine.exec.TestDownstream;
import org.apache.ignite.internal.sql.engine.exec.UpdatableTable;
import org.apache.ignite.internal.sql.engine.exec.mapping.ColocationGroup;
import org.apache.ignite.internal.sql.engine.exec.mapping.FragmentDescription;
import org.apache.ignite.internal.sql.engine.exec.row.BaseTypeSpec;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.sql.engine.framework.DataProvider;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptorImpl;
import org.apache.ignite.internal.sql.engine.schema.DefaultValueStrategy;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptorImpl;
import org.apache.ignite.internal.sql.engine.trait.IgniteDistributions;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
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

    private static final RowSchema INT_LONG_SCHEMA = RowSchema.builder()
            .addField(NativeTypes.INT32)
            .addField(NativeTypes.INT64)
            .build();

    @SuppressWarnings("OverridableMethodCallDuringObjectConstruction")
    private final RowHandler<RowWrapper> handler = rowHandler();
    private final RowHandler.RowFactory<RowWrapper> rowFactory = handler.factory(INT_LONG_SCHEMA);

    @Mock
    private UpdatableTable updatableTable;

    @BeforeEach
    void setUpMock() {
        RowSchema rowSchema = RowSchema.builder()
                .addField(NativeTypes.INT32)
                .addField(NativeTypes.INT64)
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
                context, updatableTable, SOURCE_ID, Operation.INSERT, null
        );

        TestDownstream<RowWrapper> downstream = new TestDownstream<>();

        modifyNode.register(List.of(sourceNode));
        modifyNode.onRegister(downstream);

        if (sourceSize > 0) {
            when(updatableTable.insertAll(any(), any(), any()))
                    .thenReturn(nullCompletedFuture());
        }

        context.execute(() -> modifyNode.request(1), modifyNode::onError);

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
                context, updatableTable, SOURCE_ID, Operation.UPDATE, null
        );

        TestDownstream<RowWrapper> downstream = new TestDownstream<>();

        modifyNode.register(List.of(sourceNode));
        modifyNode.onRegister(downstream);

        if (sourceSize > 0) {
            when(updatableTable.upsertAll(any(), any(), any()))
                    .thenReturn(nullCompletedFuture());
        }

        context.execute(() -> modifyNode.request(1), modifyNode::onError);

        List<RowWrapper> result = await(downstream.result());

        assertThat(result, notNullValue());
        assertThat(result.get(0), notNullValue());
        assertThat(handler.get(0, result.get(0)), is((long) sourceSize));
        verify(updatableTable, times(numberOfBatches(sourceSize))).upsertAll(any(), any(), any());
        verify(updatableTable, times(2)).descriptor();
        verifyNoMoreInteractions(updatableTable);
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, MODIFY_BATCH_SIZE - 1, MODIFY_BATCH_SIZE, MODIFY_BATCH_SIZE + 1, 2 * MODIFY_BATCH_SIZE})
    void nodeReportsExpectedNumberOfUpdatedRowsOnDelete(int sourceSize) {
        ExecutionContext<RowWrapper> context = executionContext();

        Node<RowWrapper> sourceNode = createSource(sourceSize, context);

        ModifyNode<RowWrapper> modifyNode = new ModifyNode<>(
                context, updatableTable, SOURCE_ID, Operation.DELETE, null
        );

        TestDownstream<RowWrapper> downstream = new TestDownstream<>();

        modifyNode.register(List.of(sourceNode));
        modifyNode.onRegister(downstream);

        if (sourceSize > 0) {
            when(updatableTable.deleteAll(any(), any(), any()))
                    .thenReturn(nullCompletedFuture());
        }

        context.execute(() -> modifyNode.request(1), modifyNode::onError);

        List<RowWrapper> result = await(downstream.result());

        assertThat(result, notNullValue());
        assertThat(result.get(0), notNullValue());
        assertThat(handler.get(0, result.get(0)), is((long) sourceSize));
        verify(updatableTable, times(numberOfBatches(sourceSize))).deleteAll(any(), any(), any());
        verify(updatableTable, times(2)).descriptor();
        verifyNoMoreInteractions(updatableTable);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, MODIFY_BATCH_SIZE - 1, MODIFY_BATCH_SIZE, MODIFY_BATCH_SIZE + 1, 2 * MODIFY_BATCH_SIZE})
    void exceptionIsPassedThroughToErrorHandlerOnInsert(int sourceSize) {
        ExecutionContext<RowWrapper> context = executionContext();

        Node<RowWrapper> sourceNode = createSource(sourceSize, context);

        ModifyNode<RowWrapper> modifyNode = new ModifyNode<>(
                context, updatableTable, SOURCE_ID, Operation.INSERT, null
        );

        TestDownstream<RowWrapper> downstream = new TestDownstream<>();

        modifyNode.register(List.of(sourceNode));
        modifyNode.onRegister(downstream);

        RuntimeException expected = new RuntimeException("this is expected");
        when(updatableTable.insertAll(any(), any(), any()))
                .thenReturn(CompletableFuture.failedFuture(expected));

        context.execute(() -> modifyNode.request(1), modifyNode::onError);

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
                context, updatableTable, SOURCE_ID, Operation.UPDATE, null
        );

        TestDownstream<RowWrapper> downstream = new TestDownstream<>();

        modifyNode.register(List.of(sourceNode));
        modifyNode.onRegister(downstream);

        RuntimeException expected = new RuntimeException("this is expected");
        when(updatableTable.upsertAll(any(), any(), any()))
                .thenReturn(CompletableFuture.failedFuture(expected));

        context.execute(() -> modifyNode.request(1), modifyNode::onError);

        assertThat(downstream.result(), willThrow(is(expected)));
        verify(updatableTable).upsertAll(any(), any(), any());
        verify(updatableTable, times(2)).descriptor();
        verifyNoMoreInteractions(updatableTable);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, MODIFY_BATCH_SIZE - 1, MODIFY_BATCH_SIZE, MODIFY_BATCH_SIZE + 1, 2 * MODIFY_BATCH_SIZE})
    void exceptionIsPassedThroughToErrorHandlerOnDelete(int sourceSize) {
        ExecutionContext<RowWrapper> context = executionContext();

        Node<RowWrapper> sourceNode = createSource(sourceSize, context);

        ModifyNode<RowWrapper> modifyNode = new ModifyNode<>(
                context, updatableTable, SOURCE_ID, Operation.DELETE, null
        );

        TestDownstream<RowWrapper> downstream = new TestDownstream<>();

        modifyNode.register(List.of(sourceNode));
        modifyNode.onRegister(downstream);

        RuntimeException expected = new RuntimeException("this is expected");
        when(updatableTable.deleteAll(any(), any(), any()))
                .thenReturn(CompletableFuture.failedFuture(expected));

        context.execute(() -> modifyNode.request(1), modifyNode::onError);

        assertThat(downstream.result(), willThrow(is(expected)));
        verify(updatableTable).deleteAll(any(), any(), any());
        verify(updatableTable, times(2)).descriptor();
        verifyNoMoreInteractions(updatableTable);
    }

    @ParameterizedTest
    @ValueSource(ints = {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13})
    void mergePassesCorrectRowsToInsert(int rowCount) {
        // DestinationRow: dst_c1, dst_c2, dst_c3
        // SourceRow: src_c1, src_c2
        // MergeRow:  src_c1, src_c2, null, dst_c1, dst_c2, dst_c3, update_col1, ...,

        ExecutionContext<RowWrapper> context = executionContext();
        RowHandler<RowWrapper> rowHandler = context.rowHandler();

        RowSchema.Builder dstRowSchemaBuilder = RowSchema.builder();

        for (int i = 0; i < rowCount; i++) {
            dstRowSchemaBuilder.addField(NativeTypes.INT32, true);
        }

        RowSchema dstRowSchema = dstRowSchemaBuilder.build();

        RowSchema.Builder srcRowSchemaBuilder = RowSchema.builder();

        for (int i = 0; i < rowCount; i++) {
            srcRowSchemaBuilder.addField(NativeTypes.INT32, true);
        }

        RowSchema srcRowSchema = srcRowSchemaBuilder.build();

        RowSchema updateSchema = RowSchema.builder()
                .addField(NativeTypes.INT32, true)
                .build();

        Mockito.reset(updatableTable);

        TableDescriptor tableDescriptor = createTableDescriptor(dstRowSchema);
        ArgumentCaptor<List<RowWrapper>> insertedRows = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<List<RowWrapper>> updatedRows = ArgumentCaptor.forClass(List.class);

        when(updatableTable.descriptor()).thenReturn(tableDescriptor);
        when(updatableTable.insertAll(any(), insertedRows.capture(), any())).thenReturn(nullCompletedFuture());
        when(updatableTable.upsertAll(any(), updatedRows.capture(), any())).thenReturn(nullCompletedFuture());

        RowFactory<RowWrapper> dstFactory = rowHandler.factory(dstRowSchema);

        Object[] dstRow1Data = new Object[rowCount];
        dstRow1Data[0] = 1;
        RowWrapper dstRow1 = dstFactory.create(dstRow1Data);

        Object[] dstRow2Data = new Object[rowCount];
        RowWrapper dstRow2 = dstFactory.create(dstRow2Data);

        RowFactory<RowWrapper> srcFactory = rowHandler.factory(srcRowSchema);

        Object[] srcRow1Data = new Object[rowCount];
        srcRow1Data[0] = 2;
        RowWrapper srcRow1 = srcFactory.create(srcRow1Data);

        Object[] srcRow2Data = new Object[rowCount];
        srcRow2Data[0] = 1;
        srcRow2Data[1] = 5;
        RowWrapper srcRow2 = srcFactory.create(srcRow2Data);

        RowFactory<RowWrapper> updateFactory = rowHandler.factory(updateSchema);

        RowWrapper update = updateFactory.create(4);
        RowWrapper noUpdate = updateFactory.create(new Object[]{null});

        RowWrapper mergeRow1 = rowHandler.concat(rowHandler.concat(srcRow1, dstRow1), noUpdate);
        RowWrapper mergeRow2 = rowHandler.concat(rowHandler.concat(srcRow2, dstRow2), update);

        Node<RowWrapper> sourceNode = new ScanNode<>(
                context, DataProvider.fromCollection(List.of(mergeRow1, mergeRow2))
        );

        TestDownstream<RowWrapper> downstream = new TestDownstream<>();

        ModifyNode<RowWrapper> modifyNode = new ModifyNode<>(
                context, updatableTable, SOURCE_ID, Operation.MERGE, List.of("C1")
        );
        modifyNode.register(List.of(sourceNode));
        modifyNode.onRegister(downstream);

        context.execute(() -> modifyNode.request(1), modifyNode::onError);

        await(downstream.result());

        verify(updatableTable).insertAll(any(), any(), any());
        verify(updatableTable).upsertAll(any(), any(), any());

        RowWrapper inserted = insertedRows.getAllValues().get(0).get(0);
        expectRow(inserted, rowHandler, rowCount, 1, 5);

        RowWrapper updated = updatedRows.getAllValues().get(0).get(0);
        expectRow(updated, rowHandler, rowCount, 1, null);
    }

    private static TableDescriptor createTableDescriptor(RowSchema rowSchema) {
        List<ColumnDescriptor> columns = new ArrayList<>();

        for (int i = 0; i < rowSchema.fields().size(); i++) {
            BaseTypeSpec typeSpec = (BaseTypeSpec) rowSchema.fields().get(i);
            ColumnDescriptorImpl col = new ColumnDescriptorImpl(
                    "C" + i,
                    false,
                    false,
                    false,
                    typeSpec.isNullable(),
                    i,
                    typeSpec.nativeType(),
                    DefaultValueStrategy.DEFAULT_NULL,
                    null
            );
            columns.add(col);
        }

        return new TableDescriptorImpl(columns, IgniteDistributions.single());
    }

    private static void expectRow(RowWrapper row, RowHandler<RowWrapper> rowHandler, int expectedRowSize, Object... expectRowPrefix) {
        int rowSize = rowHandler.columnCount(row);

        assertEquals(expectedRowSize, rowSize);
        assertTrue(expectRowPrefix.length <= rowSize, "Incorrect number of expected vals");

        for (int i = 0; i < rowSize; i++) {
            if (i < expectRowPrefix.length) {
                assertEquals(expectRowPrefix[i], rowHandler.get(i, row), "col#" + i);
            } else {
                assertNull(rowHandler.get(i, row), "col#" + i);
            }
        }
    }

    private static int numberOfBatches(int rowCount) {
        return rowCount / MODIFY_BATCH_SIZE + (rowCount % MODIFY_BATCH_SIZE == 0 ? 0 : 1);
    }

    private Node<RowWrapper> createSource(int rowCount, ExecutionContext<RowWrapper> context) {
        return new ScanNode<>(
                context, DataProvider.fromRow(rowFactory.create(1, 1L), rowCount)
        );
    }

    @Override
    protected RowHandler<RowWrapper> rowHandler() {
        return SqlRowHandler.INSTANCE;
    }

    @Override
    protected FragmentDescription getFragmentDescription() {
        ColocationGroup colocationGroup = new ColocationGroup(List.of(), List.of(), Int2ObjectMaps.emptyMap());
        return new FragmentDescription(0, true, Long2ObjectMaps.singleton(SOURCE_ID, colocationGroup), null, null, null);
    }
}
