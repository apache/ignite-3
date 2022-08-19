/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql.engine.exec.schema;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.calcite.schema.Table;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.registry.SchemaRegistryImpl;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.IgniteTableImpl;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManagerImpl;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.NodeStoppingException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests to verify {@link SqlSchemaManagerImpl}.
 */
@ExtendWith(MockitoExtension.class)
public class SqlSchemaManagerTest {
    private final UUID tableId = UUID.randomUUID();

    private final int tableVer = 1;

    private final SchemaDescriptor schemaDescriptor = new SchemaDescriptor(
            tableVer,
            new Column[]{new Column(0, "ID", NativeTypes.INT64, false)},
            new Column[]{new Column(1, "VAL", NativeTypes.INT64, false)}
        );

    @Mock
    private TableManager tableManager;

    @Mock
    private SchemaManager schemaManager;

    @Mock
    private TableImpl table;

    @Mock
    SchemaRegistryImpl schemaRegistry;

    private SqlSchemaManagerImpl sqlSchemaManager;

    private TestRevisionRegister testRevisionRegister;

    /** Busy lock for stop synchronisation. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    @BeforeEach
    public void setup() throws NodeStoppingException {
        Mockito.reset(tableManager);

        testRevisionRegister = new TestRevisionRegister();

        sqlSchemaManager = new SqlSchemaManagerImpl(
                tableManager,
                schemaManager,
                testRevisionRegister,
                busyLock
        );

        testRevisionRegister.moveForward();
    }

    @Test
    public void testNonExistingTable() throws NodeStoppingException {
        UUID tblId = UUID.randomUUID();

        IgniteInternalException ex = assertThrows(IgniteInternalException.class, () -> sqlSchemaManager.tableById(tblId, tableVer));
        assertThat(ex.getMessage(), containsString("Table not found"));

        Mockito.verify(tableManager).table(eq(tblId));

        Mockito.verifyNoMoreInteractions(tableManager);
    }

    @Test
    public void testTableEventIsNotProcessed() throws NodeStoppingException {
        when(tableManager.table(eq(tableId))).thenReturn(table);
        when(table.schemaView()).thenReturn(schemaRegistry);

        InternalTable mock = mock(InternalTable.class);
        when(mock.tableId()).thenReturn(tableId);

        when(table.internalTable()).thenReturn(mock);
        when(schemaRegistry.schema()).thenReturn(schemaDescriptor);
        when(schemaRegistry.lastSchemaVersion()).thenReturn(schemaDescriptor.version());

        when(schemaManager.schemaRegistry(any())).thenReturn(schemaRegistry);

        IgniteTable actTable = sqlSchemaManager.tableById(tableId, tableVer);

        assertEquals(tableId, actTable.id());

        Mockito.verify(tableManager).table(eq(tableId));

        Mockito.verifyNoMoreInteractions(tableManager);
    }

    @Test
    public void testTableEventIsProcessedRequiredVersionIsSame() {
        when(table.name()).thenReturn("TEST_SCHEMA.T");

        InternalTable mock = mock(InternalTable.class);
        when(mock.tableId()).thenReturn(tableId);

        when(table.internalTable()).thenReturn(mock);
        when(schemaRegistry.schema()).thenReturn(schemaDescriptor);
        when(schemaRegistry.lastSchemaVersion()).thenReturn(schemaDescriptor.version());

        when(schemaManager.schemaRegistry(anyLong(), any())).thenReturn(completedFuture(schemaRegistry));

        sqlSchemaManager.onTableCreated("TEST_SCHEMA", table, testRevisionRegister.actualToken() + 1);
        testRevisionRegister.moveForward();

        IgniteTable actTable = sqlSchemaManager.tableById(tableId, tableVer);

        assertEquals(tableId, actTable.id());

        Mockito.verifyNoMoreInteractions(tableManager);
    }

    @Test
    public void testTableEventIsProcessedRequiredVersionIsLess() {
        when(table.name()).thenReturn("TEST_SCHEMA.T");

        InternalTable mock = mock(InternalTable.class);
        when(mock.tableId()).thenReturn(tableId);

        when(table.internalTable()).thenReturn(mock);
        when(schemaRegistry.schema()).thenReturn(schemaDescriptor);
        when(schemaRegistry.lastSchemaVersion()).thenReturn(schemaDescriptor.version());

        when(schemaManager.schemaRegistry(anyLong(), any())).thenReturn(completedFuture(schemaRegistry));

        sqlSchemaManager.onTableCreated("TEST_SCHEMA", table, testRevisionRegister.actualToken() + 1);
        testRevisionRegister.moveForward();

        IgniteTable actTable = sqlSchemaManager.tableById(tableId, tableVer - 1);

        assertEquals(tableId, actTable.id());

        Mockito.verifyNoMoreInteractions(tableManager);
    }

    @Test
    public void testTableEventIsProcessedRequiredVersionIsGreater() throws NodeStoppingException {
        when(table.schemaView()).thenReturn(schemaRegistry);
        when(table.name()).thenReturn("TEST_SCHEMA.T");

        InternalTable mock = mock(InternalTable.class);
        when(mock.tableId()).thenReturn(tableId);

        when(table.internalTable()).thenReturn(mock);
        when(schemaRegistry.schema()).thenReturn(schemaDescriptor);
        when(schemaRegistry.lastSchemaVersion()).thenReturn(tableVer - 1);
        when(schemaManager.schemaRegistry(anyLong(), any())).thenReturn(completedFuture(schemaRegistry));
        when(schemaManager.schemaRegistry(any())).thenReturn(schemaRegistry);

        sqlSchemaManager.onTableCreated("TEST_SCHEMA", table, testRevisionRegister.actualToken() + 1);
        testRevisionRegister.moveForward();

        when(tableManager.table(eq(tableId))).thenReturn(table);
        when(schemaRegistry.lastSchemaVersion()).thenReturn(tableVer);

        IgniteTable actTable = sqlSchemaManager.tableById(tableId, tableVer);
        assertEquals(tableId, actTable.id());

        IgniteInternalException ex = assertThrows(IgniteInternalException.class, () -> sqlSchemaManager.tableById(tableId, tableVer + 1));
        assertThat(ex.getMessage(), containsString("Table version not found"));

        Mockito.verify(tableManager, times(2)).table(eq(tableId));

        Mockito.verifyNoMoreInteractions(tableManager);
    }

    @Test
    public void testOnTableDroppedHandler() {
        when(table.name()).thenReturn("TEST_SCHEMA.T");

        InternalTable mock = mock(InternalTable.class);
        when(mock.tableId()).thenReturn(tableId);

        when(table.internalTable()).thenReturn(mock);
        when(schemaRegistry.schema()).thenReturn(schemaDescriptor);
        when(schemaRegistry.lastSchemaVersion()).thenReturn(schemaDescriptor.version());

        when(schemaManager.schemaRegistry(anyLong(), any())).thenReturn(completedFuture(schemaRegistry));

        sqlSchemaManager.onTableCreated("TEST_SCHEMA", table, testRevisionRegister.actualToken() + 1);
        testRevisionRegister.moveForward();

        Table schemaTable = sqlSchemaManager.schema("TEST_SCHEMA").getTable("T");

        assertNotNull(schemaTable);
        IgniteTableImpl igniteTable = assertInstanceOf(IgniteTableImpl.class, schemaTable);
        assertEquals(tableId, igniteTable.table().tableId());

        sqlSchemaManager.onTableDropped("TEST_SCHEMA", table.name(), testRevisionRegister.actualToken() + 1);
        testRevisionRegister.moveForward();

        assertNull(sqlSchemaManager.schema("TEST_SCHEMA").getTable("T"));
    }

    /**
     * Test revision register.
     */
    private static class TestRevisionRegister implements Consumer<Function<Long, CompletableFuture<?>>> {
        AtomicLong token = new AtomicLong(-1);

        /** Revision consumer. */
        private Function<Long, CompletableFuture<?>> moveRevision;

        /**
         * Moves forward token.
         */
        void moveForward() {
            moveRevision.apply(token.incrementAndGet()).join();
        }

        /**
         * Gets an actual token.
         *
         * @return Actual token.
         */
        long actualToken() {
            return token.get();
        }

        /** {@inheritDoc} */
        @Override
        public void accept(Function<Long, CompletableFuture<?>> function) {
            if (moveRevision == null) {
                moveRevision = function;
            } else {
                Function<Long, CompletableFuture<?>> old = moveRevision;

                moveRevision = rev -> allOf(
                    old.apply(rev),
                    function.apply(rev)
                );
            }
        }
    }
}
