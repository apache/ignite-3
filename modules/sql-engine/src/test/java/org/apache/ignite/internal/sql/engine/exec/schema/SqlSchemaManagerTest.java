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

package org.apache.ignite.internal.sql.engine.exec.schema;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.ignite.internal.index.Index;
import org.apache.ignite.internal.index.IndexDescriptor;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.registry.SchemaRegistryImpl;
import org.apache.ignite.internal.sql.engine.planner.AbstractPlannerTest.TestHashIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteSchema;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.IgniteTableImpl;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManagerImpl;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
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
public class SqlSchemaManagerTest extends BaseIgniteAbstractTest {
    private final int tableId = 1;

    private final int indexId = 2;

    private final SchemaDescriptor schemaDescriptor = new SchemaDescriptor(
            1,
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
    private SchemaRegistryImpl schemaRegistry;

    private SqlSchemaManagerImpl sqlSchemaManager;

    private TestRevisionRegister testRevisionRegister;

    /** Busy lock for stop synchronisation. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    @BeforeEach
    public void setup() {
        Mockito.reset(tableManager);

        when(tableManager.tableAsync(anyLong(), eq(tableId))).thenReturn(completedFuture(table));

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
    public void testOnTableDroppedHandler() {
        InternalTable mock = mock(InternalTable.class);
        when(mock.tableId()).thenReturn(tableId);
        when(mock.name()).thenReturn("T");

        when(table.internalTable()).thenReturn(mock);
        when(table.tableId()).thenReturn(tableId);
        when(schemaRegistry.schema()).thenReturn(schemaDescriptor);
        when(schemaRegistry.lastSchemaVersion()).thenReturn(schemaDescriptor.version());

        when(schemaManager.schemaRegistry(anyLong(), anyInt())).thenReturn(completedFuture(schemaRegistry));

        sqlSchemaManager.onTableCreated("PUBLIC", tableId, testRevisionRegister.actualToken() + 1);
        testRevisionRegister.moveForward();

        TestHashIndex testHashIndex = TestHashIndex.create(List.of("ID"), "pk_idx", tableId);

        sqlSchemaManager.onIndexCreated(
                testHashIndex.tableId(),
                testHashIndex.id(),
                testHashIndex.descriptor(),
                testRevisionRegister.actualToken() + 1
        );

        testRevisionRegister.moveForward();

        Table schemaTable = sqlSchemaManager.latestSchema("PUBLIC").getTable("T");

        assertNotNull(schemaTable);
        IgniteTableImpl igniteTable = assertInstanceOf(IgniteTableImpl.class, schemaTable);
        assertEquals(tableId, igniteTable.id());

        sqlSchemaManager.onTableDropped("PUBLIC", tableId, testRevisionRegister.actualToken() + 1);
        testRevisionRegister.moveForward();

        assertNull(sqlSchemaManager.latestSchema("PUBLIC").getTable("T"));
    }

    @Test
    public void testIndexEventHandler() {
        InternalTable mock = mock(InternalTable.class);
        when(mock.tableId()).thenReturn(tableId);
        when(mock.name()).thenReturn("T");

        when(table.internalTable()).thenReturn(mock);
        when(table.tableId()).thenReturn(tableId);
        when(schemaRegistry.schema()).thenReturn(schemaDescriptor);
        when(schemaRegistry.lastSchemaVersion()).thenReturn(schemaDescriptor.version());
        when(schemaManager.schemaRegistry(anyLong(), anyInt())).thenReturn(completedFuture(schemaRegistry));

        sqlSchemaManager.onTableCreated("PUBLIC", tableId, testRevisionRegister.actualToken() + 1);
        testRevisionRegister.moveForward();

        TestHashIndex testHashIndex = TestHashIndex.create(List.of("ID"), "pk_idx", tableId);

        sqlSchemaManager.onIndexCreated(
                testHashIndex.tableId(),
                testHashIndex.id(),
                testHashIndex.descriptor(),
                testRevisionRegister.actualToken() + 1
        );

        testRevisionRegister.moveForward();

        assertEquals(1, ((IgniteTableImpl) sqlSchemaManager.latestSchema("PUBLIC").getTable("T")).indexes().size());

        IndexDescriptor descMock = mock(IndexDescriptor.class);
        when(descMock.columns()).thenReturn(List.of());
        when(descMock.name()).thenReturn("PUBLIC.I");

        sqlSchemaManager.onIndexCreated(tableId, indexId, descMock, testRevisionRegister.actualToken() + 1);

        testRevisionRegister.moveForward();

        IgniteSchema schema = sqlSchemaManager.latestSchema("PUBLIC").unwrap(IgniteSchema.class);
        Table schemaTable = schema.getTable("T");
        IgniteIndex igniteIndex = schema.index(indexId);

        assertNotNull(igniteIndex);

        IgniteTableImpl igniteTable = assertInstanceOf(IgniteTableImpl.class, schemaTable);

        assertEquals(igniteTable.id(), igniteIndex.tableId());
        assertSame(igniteIndex, igniteTable.indexes().get("PUBLIC.I"));

        sqlSchemaManager.onIndexDropped("PUBLIC", igniteTable.id(), indexId, testRevisionRegister.actualToken() + 1);
        testRevisionRegister.moveForward();

        assertNull(sqlSchemaManager.latestSchema("PUBLIC").unwrap(IgniteSchema.class).index(indexId));

        verifyNoMoreInteractions(tableManager);
    }


    @Test
    public void testIndexEventsProcessed() {
        InternalTable mock = mock(InternalTable.class);
        when(mock.tableId()).thenReturn(tableId);
        when(mock.name()).thenReturn("T");

        when(table.internalTable()).thenReturn(mock);
        when(table.tableId()).thenReturn(tableId);
        when(schemaRegistry.schema()).thenReturn(schemaDescriptor);
        when(schemaRegistry.lastSchemaVersion()).thenReturn(schemaDescriptor.version());
        when(schemaManager.schemaRegistry(anyLong(), anyInt())).thenReturn(completedFuture(schemaRegistry));

        sqlSchemaManager.onTableCreated("PUBLIC", table.tableId(), testRevisionRegister.actualToken() + 1);
        testRevisionRegister.moveForward();

        TestHashIndex testHashIndex = TestHashIndex.create(List.of("ID"), "pk_idx", tableId);

        sqlSchemaManager.onIndexCreated(
                testHashIndex.tableId(),
                testHashIndex.id(),
                testHashIndex.descriptor(),
                testRevisionRegister.actualToken() + 1
        );

        testRevisionRegister.moveForward();

        String idxName = "I";

        IndexDescriptor descMock = mock(IndexDescriptor.class);
        when(descMock.columns()).thenReturn(List.of());
        when(descMock.name()).thenReturn(idxName);

        {
            SchemaPlus schema1 = sqlSchemaManager.latestSchema("PUBLIC");

            sqlSchemaManager.onIndexCreated(tableId, indexId, descMock, testRevisionRegister.actualToken() + 1);
            testRevisionRegister.moveForward();

            SchemaPlus schema2 = sqlSchemaManager.latestSchema("PUBLIC");

            // Validate schema snapshot.
            assertNotSame(schema1, schema2);
            assertNotSame(schema1.getTable("T"), schema2.getTable("T"));

            assertNull(schema1.unwrap(IgniteSchema.class).index(indexId));
            assertNotNull(schema2.unwrap(IgniteSchema.class).index(indexId));

            assertNull(((IgniteTable) schema1.getTable("T")).getIndex(idxName));
            assertNotNull(((IgniteTable) schema2.getTable("T")).getIndex(idxName));
        }
        {
            sqlSchemaManager.onIndexDropped("PUBLIC", table.tableId(), indexId, testRevisionRegister.actualToken() + 1);
            SchemaPlus schema1 = sqlSchemaManager.latestSchema("PUBLIC");
            testRevisionRegister.moveForward();

            SchemaPlus schema2 = sqlSchemaManager.latestSchema("PUBLIC");

            // Validate schema snapshot.
            assertNotSame(schema1, schema2);
            assertNotSame(schema1.getTable("T"), schema2.getTable("T"));

            assertNotNull(schema1.unwrap(IgniteSchema.class).index(indexId));
            assertNull(schema2.unwrap(IgniteSchema.class).index(indexId));

            assertNull(((IgniteTable) schema2.getTable("T")).getIndex(idxName));
            assertNotNull(((IgniteTable) schema1.getTable("T")).getIndex(idxName));
        }

        verifyNoMoreInteractions(tableManager);
    }

    /**
     * Test revision register.
     */
    private static class TestRevisionRegister implements Consumer<LongFunction<CompletableFuture<?>>> {
        AtomicLong token = new AtomicLong(-1);

        /** Revision consumer. */
        private LongFunction<CompletableFuture<?>> moveRevision;

        /**
         * Moves forward token.
         */
        void moveForward() {
            await(moveRevision.apply(token.incrementAndGet()));
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
        public void accept(LongFunction<CompletableFuture<?>> function) {
            if (moveRevision == null) {
                moveRevision = function;
            } else {
                LongFunction<CompletableFuture<?>> old = moveRevision;

                moveRevision = rev -> allOf(
                        old.apply(rev),
                        function.apply(rev)
                );
            }
        }
    }
}
