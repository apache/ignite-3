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

package org.apache.ignite.internal.sql.engine.exec;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.TestHybridClock;
import org.apache.ignite.internal.components.SystemPropertiesNodeProperties;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.TestClockService;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.sql.engine.framework.TestStatistic;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.IgniteTableImpl;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.util.cache.CaffeineCacheFactory;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.schema.ConstantSchemaVersions;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.internal.tx.impl.HeapLockManager;
import org.apache.ignite.internal.tx.impl.WaitDieDeadlockPreventionPolicy;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.sql.IgniteSql;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@link ExecutableTableRegistryImpl}.
 */
@ExtendWith(MockitoExtension.class)
public class ExecutableTableRegistrySelfTest extends BaseIgniteAbstractTest {

    @Mock
    private ReplicaService replicaService;

    @Mock
    private TableManager tableManager;

    @Mock
    private SchemaManager schemaManager;

    @Mock
    private SqlSchemaManager sqlSchemaManager;

    @Mock
    private TableDescriptor descriptor;

    @Mock
    private InternalTable internalTable;

    @Mock
    private SchemaRegistry schemaRegistry;

    private final HybridClock clock = new TestHybridClock(() -> 1000);

    /**
     * Test table loading.
     */
    @Test
    public void testGetTable() {
        Tester tester = new Tester();

        int tableId = 1;

        ExecutableTable executableTable = tester.getTable(tableId);

        assertNotNull(executableTable.scannableTable());
        assertNotNull(executableTable.updatableTable());
    }

    /** Entries are removed from cache when cache capacity is reached. */
    @Test
    public void testEntriesAreRemovedFromCache() throws InterruptedException {
        int cacheSize = 2;
        Tester tester = new Tester(cacheSize);

        tester.getTable(1);
        tester.getTable(2);
        tester.getTable(3);

        boolean done = IgniteTestUtils.waitForCondition(() -> tester.registry.tableCache.size() == cacheSize, 15_000);
        assertTrue(done, "Failed to clear the cache");
    }

    private static SchemaDescriptor newDescriptor(int schemaVersion) {
        return new SchemaDescriptor(
                schemaVersion,
                new Column[]{new Column("key", NativeTypes.INT64, false)},
                new Column[]{new Column("val", NativeTypes.INT64, true)}
        );
    }

    private class Tester {

        ExecutableTableRegistryImpl registry;

        Tester() {
            this(Integer.MAX_VALUE);
        }

        Tester(int cacheSize) {
            registry = new ExecutableTableRegistryImpl(
                    tableManager,
                    schemaManager,
                    sqlSchemaManager,
                    replicaService,
                    new TestClockService(clock),
                    new SystemPropertiesNodeProperties(),
                    cacheSize,
                    CaffeineCacheFactory.INSTANCE
            );
        }

        ExecutableTable getTable(int tableId) {
            int schemaVersion = 1;
            int tableVersion = 10;

            TableImpl table = new TableImpl(internalTable, schemaRegistry, lockManager(), new ConstantSchemaVersions(tableVersion),
                    mock(IgniteSql.class), -1);

            SchemaDescriptor schemaDescriptor = newDescriptor(schemaVersion);

            when(tableManager.cachedTable(tableId)).thenReturn(table);
            when(schemaManager.schemaRegistry(tableId)).thenReturn(schemaRegistry);
            when(schemaRegistry.schema(tableVersion)).thenReturn(schemaDescriptor);

            IgniteTable sqlTable = new IgniteTableImpl(
                    "TBL1", tableId, tableVersion, tableVersion, descriptor, ImmutableIntList.of(0),
                    new TestStatistic(1_000.0), Map.of(), 1, 10000
            );

            when(sqlSchemaManager.table(schemaVersion, tableId)).thenReturn(sqlTable);

            return registry.getTable(schemaVersion, tableId);
        }

        private LockManager lockManager() {
            HeapLockManager lockManager = HeapLockManager.smallInstance();
            lockManager.start(new WaitDieDeadlockPreventionPolicy());
            return lockManager;
        }
    }
}
