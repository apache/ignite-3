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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.longs.Long2ObjectMaps;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory.Builder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler;
import org.apache.ignite.internal.sql.engine.exec.SqlRowHandler.RowWrapper;
import org.apache.ignite.internal.sql.engine.exec.TestDownstream;
import org.apache.ignite.internal.sql.engine.exec.UpdatableTable;
import org.apache.ignite.internal.sql.engine.exec.mapping.ColocationGroup;
import org.apache.ignite.internal.sql.engine.exec.mapping.FragmentDescription;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.sql.engine.framework.DataProvider;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.internal.type.NativeTypes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
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

    @Mock
    private TableDescriptor descriptors;

    @BeforeEach
    void setUpMock() {
        when(descriptors.columnsCount()).thenReturn(2);
        when(updatableTable.descriptor()).thenReturn(descriptors);

        Builder rowTypeBuilder = new Builder(Commons.typeFactory());

        rowTypeBuilder = rowTypeBuilder.add("col1", SqlTypeName.INTEGER)
                .add("col2", SqlTypeName.BIGINT);

        RelDataType rowType =  rowTypeBuilder.build();
        when(descriptors.rowType(any(), any())).thenReturn(rowType);
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
        verify(updatableTable, times(3)).descriptor();
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
        verify(updatableTable, times(3)).descriptor();
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
        verify(updatableTable, times(3)).descriptor();
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
        verify(updatableTable, times(3)).descriptor();
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
        verify(updatableTable, times(3)).descriptor();
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
        verify(updatableTable, times(3)).descriptor();
        verifyNoMoreInteractions(updatableTable);
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
