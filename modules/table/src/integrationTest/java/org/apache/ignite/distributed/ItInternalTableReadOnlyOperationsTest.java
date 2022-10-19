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

package org.apache.ignite.distributed;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.PartitionTimestampCursor;
import org.apache.ignite.internal.storage.ReadResult;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.impl.DummyInternalTableImpl;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.tx.TransactionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@link InternalTable} read-only operations.
 */
@ExtendWith(MockitoExtension.class)
public class ItInternalTableReadOnlyOperationsTest {
    private static final SchemaDescriptor SCHEMA = new SchemaDescriptor(
            1,
            new Column[]{new Column("key", NativeTypes.INT64, false)},
            new Column[]{new Column("value", NativeTypes.INT64, false)}
    );

    private static final Row ROW_1 = createKeyValueRow(1, 1001);

    private static final Row ROW_2 = createKeyValueRow(2, 1002);


    /** Mock partition storage. */
    @Mock
    private MvPartitionStorage mockStorage;

    /** Internal table to test. */
    private InternalTable internalTbl;

    /**
     * Prepare test environment using DummyInternalTableImpl and Mocked storage.
     */
    @BeforeEach
    public void setUp(TestInfo testInfo) {
        internalTbl = new DummyInternalTableImpl(Mockito.mock(ReplicaService.class), mockStorage);

        mockStorage(List.of(ROW_1, ROW_2));

    }

    @Test
    public void testReadOnlyGetNonExistingKey() {
        assertNull(internalTbl.get(createKeyRow(0), null, mock(ClusterNode.class)).join());
    }

    @Test
    public void testReadOnlyGetExistingKey() {
        assertEquals(ROW_2, internalTbl.get(createKeyRow(2), null, mock(ClusterNode.class)).join());
    }


    @Test
    public void testReadOnlyGetAllNonExistingKeys() {
        assertEquals(0,
                internalTbl.getAll(Collections.singleton(createKeyRow(0)), null, mock(ClusterNode.class)).join().size()
        );
    }

    @Test
    public void testReadOnlyGetAllPartiallyExistingKeys() {
        assertEquals(
                Collections.singletonList(ROW_2),
                internalTbl.getAll(Collections.singleton(createKeyRow(2)), null, mock(ClusterNode.class)).join()
        );
    }

    @Test
    public void testReadOnlyGetAllExistingKeys() {
        assertEquals(
                List.of(ROW_1, ROW_2),
                internalTbl.getAll(List.of(createKeyRow(1), createKeyRow(2)), null, mock(ClusterNode.class)).join()
        );
    }

    @Test()
    public void testEnlistingReadOnlyOperationIntoReadWriteThrowsAnException() {
        InternalTransaction tx = mock(InternalTransaction.class);
        when(tx.isReadOnly()).thenReturn(false);

        List<Executable> executables = List.of(
                () -> internalTbl.get(null, tx, mock(ClusterNode.class)).get(),
                () -> internalTbl.getAll(null, tx, mock(ClusterNode.class)).get()
        );

        executables.forEach(executable -> {
            ExecutionException ex = assertThrows(ExecutionException.class, executable);

            assertThat(ex.getCause(), is(instanceOf(TransactionException.class)));
            assertThat(
                    ex.getCause().getMessage(),
                    containsString("Failed to enlist read-only get operation into read-write transaction."
                            + " Read-write transaction is up and running and thus won't be aborted automatically"));
        });

        TransactionException ex = assertThrows(TransactionException.class, () -> internalTbl.scan(0, tx, mock(ClusterNode.class)));
        assertThat(
                ex.getMessage(),
                containsString("Failed to enlist read-only get operation into read-write transaction."
                        + " Read-write transaction is up and running and thus won't be aborted automatically"));
    }

    private void mockStorage(List<BinaryRow> submittedItems) {
        // TODO: IGNITE-17859 After index integration get and getAll methods should be used instead of scan.
        AtomicInteger cursorTouchCnt = new AtomicInteger(0);

        lenient().when(mockStorage.scan(any(HybridTimestamp.class))).thenAnswer(invocation -> {
            var cursor = mock(PartitionTimestampCursor.class);

            when(cursor.hasNext()).thenAnswer(hnInvocation -> cursorTouchCnt.get() < submittedItems.size());

            when(cursor.next()).thenAnswer(ninvocation ->
                    ReadResult.createFromCommitted(submittedItems.get(cursorTouchCnt.getAndIncrement()), new HybridTimestamp(1, 0)));

            return cursor;
        });

    }

    /**
     * Creates a {@link Row} with the supplied key.
     *
     * @param id Key.
     * @return Row.
     */
    private static Row createKeyRow(long id) {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA, 0, 0);

        rowBuilder.appendLong(id);

        return new Row(SCHEMA, new ByteBufferRow(rowBuilder.toBytes()));
    }

    /**
     * Creates a {@link Row} with the supplied key and value.
     *
     * @param id    Key.
     * @param value Value.
     * @return Row.
     */
    private static Row createKeyValueRow(long id, long value) {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA, 0, 0);

        rowBuilder.appendLong(id);
        rowBuilder.appendLong(value);

        return new Row(SCHEMA, new ByteBufferRow(rowBuilder.toBytes()));
    }
}
