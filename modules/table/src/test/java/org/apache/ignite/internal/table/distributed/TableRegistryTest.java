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

package org.apache.ignite.internal.table.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

/** Tests for {@link TableRegistry}. */
class TableRegistryTest extends BaseIgniteAbstractTest {

    private final TableRegistry registry = new TableRegistry();

    @Test
    void registerAddsToTablesOnly() {
        TableViewInternal table = mock(TableViewInternal.class);

        registry.register(1, table);

        assertSame(table, registry.table(1));
        assertNull(registry.startedTable(1));
    }

    @Test
    void markStartedPromotesRegisteredTable() {
        TableViewInternal table = mock(TableViewInternal.class);

        registry.register(1, table);
        assertNull(registry.startedTable(1));

        registry.markStarted(1);
        assertSame(table, registry.startedTable(1));
    }

    @Test
    void tableReturnsNullForUnknownId() {
        assertNull(registry.table(42));
    }

    @Test
    void startedTableReturnsNullForNonStarted() {
        registry.register(1, mock(TableViewInternal.class));

        assertNull(registry.startedTable(1));
    }

    @Test
    void allTablesReturnsUnmodifiableMap() {
        registry.register(1, mock(TableViewInternal.class));

        assertThrows(UnsupportedOperationException.class, () ->
                registry.allRegisteredTables().put(2, mock(TableViewInternal.class)));
    }

    @Test
    void allStartedTablesReturnsUnmodifiableMap() {
        registry.register(1, mock(TableViewInternal.class));
        registry.markStarted(1);

        assertThrows(UnsupportedOperationException.class, () ->
                registry.allStartedTables().put(2, mock(TableViewInternal.class)));
    }

    @Test
    void allTablesReflectsRegistrations() {
        assertTrue(registry.allRegisteredTables().isEmpty());

        registry.register(1, mock(TableViewInternal.class));
        registry.register(2, mock(TableViewInternal.class));

        assertEquals(2, registry.allRegisteredTables().size());
    }

    @Test
    void allStartedTablesReflectsState() {
        assertTrue(registry.allStartedTables().isEmpty());

        registry.register(1, mock(TableViewInternal.class));
        registry.markStarted(1);

        assertEquals(1, registry.allStartedTables().size());
    }

    @Test
    void removeStartedReturnsTableAndClearsLocalPartitions() {
        TableViewInternal table = mock(TableViewInternal.class);

        registry.register(1, table);
        registry.markStarted(1);
        registry.setLocalPartitions(1, new BitSetPartitionSet());

        TableViewInternal removed = registry.removeStarted(1);

        assertSame(table, removed);
        assertNull(registry.startedTable(1));
        assertSame(PartitionSet.EMPTY_SET, registry.localPartitions(1));
        // Table should still be in the tables map.
        assertSame(table, registry.table(1));
    }

    @Test
    void removeStartedOnUnknownIdReturnsNull() {
        assertNull(registry.removeStarted(42));
    }

    @Test
    void unregisterRemovesFromTablesMap() {
        registry.register(1, mock(TableViewInternal.class));

        registry.unregister(1);

        assertNull(registry.table(1));
    }

    @Test
    void setAndGetLocalPartitions() {
        PartitionSet parts = new BitSetPartitionSet();
        parts.set(0);
        parts.set(3);

        registry.setLocalPartitions(1, parts);

        assertSame(parts, registry.localPartitions(1));
    }

    @Test
    void extendLocalPartitionsOnAbsentEntry() {
        registry.extendLocalPartitions(1, 5);

        PartitionSet parts = registry.localPartitions(1);
        assertNotNull(parts);
        assertTrue(parts.get(5));
        assertEquals(1, parts.size());
    }

    @Test
    void extendLocalPartitionsOnExistingEntry() {
        registry.extendLocalPartitions(1, 2);
        registry.extendLocalPartitions(1, 7);

        PartitionSet parts = registry.localPartitions(1);
        assertTrue(parts.get(2));
        assertTrue(parts.get(7));
        assertEquals(2, parts.size());
    }

    @Test
    void localPartitionsReturnsEmptySetIfAbsent() {
        assertSame(PartitionSet.EMPTY_SET, registry.localPartitions(99));
    }

    @Test
    void markStartedOnUnregisteredTableFails() {
        assertThrows(AssertionError.class, () -> registry.markStarted(42));
    }

    @Test
    void fullLifecycle() {
        TableViewInternal table = mock(TableViewInternal.class);

        // Register.
        registry.register(1, table);
        assertSame(table, registry.table(1));
        assertNull(registry.startedTable(1));

        // Mark started.
        registry.markStarted(1);
        assertSame(table, registry.startedTable(1));

        // Add partitions.
        registry.extendLocalPartitions(1, 0);
        assertTrue(registry.localPartitions(1).get(0));

        // Remove started (deactivate).
        TableViewInternal removed = registry.removeStarted(1);
        assertSame(table, removed);
        assertNull(registry.startedTable(1));
        assertSame(PartitionSet.EMPTY_SET, registry.localPartitions(1));
        assertSame(table, registry.table(1)); // still registered

        // Unregister (final cleanup).
        registry.unregister(1);
        assertNull(registry.table(1));
    }

    @Test
    void recoveryLifecycle() {
        TableViewInternal table = mock(TableViewInternal.class);

        // Recovery registers and starts.
        registry.register(1, table);
        registry.markStarted(1);
        assertSame(table, registry.table(1));
        assertSame(table, registry.startedTable(1));

        // Destroy.
        registry.removeStarted(1);
        registry.unregister(1);

        assertNull(registry.table(1));
        assertNull(registry.startedTable(1));
    }
}
