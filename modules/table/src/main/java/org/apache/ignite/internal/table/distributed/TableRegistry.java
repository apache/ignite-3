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

import static java.util.Collections.unmodifiableMap;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.table.TableViewInternal;
import org.jetbrains.annotations.Nullable;

/**
 * Tracks table lifecycle state shared between {@link TableManager} and {@link TableZoneCoordinator}.
 *
 * <p>A table progresses through: registered → started → (removed from started) → unregistered.
 */
class TableRegistry {
    /** All registered tables by ID. */
    private final Map<Integer, TableViewInternal> tables = new ConcurrentHashMap<>();

    /** Tables that are fully started (partition resources prepared). */
    private final Map<Integer, TableViewInternal> startedTables = new ConcurrentHashMap<>();

    /** Local partitions by table ID. */
    private final Map<Integer, PartitionSet> localPartsByTableId = new ConcurrentHashMap<>();

    /** Registers a newly created table. Does not mark it as started. */
    void register(int tableId, TableViewInternal table) {
        tables.put(tableId, table);
    }

    /** Promotes an already-registered table to the started. */
    void markStarted(int tableId) {
        TableViewInternal table = tables.get(tableId);

        assert table != null : "Table must be registered before marking as started: tableId=" + tableId;

        startedTables.put(tableId, table);
    }

    /** Returns a registered table by ID, or null if not found. */
    @Nullable TableViewInternal table(int tableId) {
        return tables.get(tableId);
    }

    /** Returns a started table by ID, or null if not started. */
    @Nullable TableViewInternal startedTable(int tableId) {
        return startedTables.get(tableId);
    }

    /** Returns an unmodifiable view of all registered tables. */
    Map<Integer, TableViewInternal> allRegisteredTables() {
        return unmodifiableMap(tables);
    }

    /** Returns an unmodifiable view of all started tables. */
    Map<Integer, TableViewInternal> allStartedTables() {
        return unmodifiableMap(startedTables);
    }

    /** Removes the table from started tables and clears its local partitions. Returns the removed table, or null. */
    @Nullable TableViewInternal removeStarted(int tableId) {
        TableViewInternal removed = startedTables.remove(tableId);
        localPartsByTableId.remove(tableId);
        return removed;
    }

    /** Removes the table from the registry entirely. */
    void unregister(int tableId) {
        tables.remove(tableId);
    }

    /** Sets the local partition set for a table. */
    void setLocalPartitions(int tableId, PartitionSet partitions) {
        localPartsByTableId.put(tableId, partitions);
    }

    /** Atomically extends local partitions by adding a partition index. Creates a new set if absent. */
    void extendLocalPartitions(int tableId, int partitionIndex) {
        localPartsByTableId.compute(tableId, (id, old) -> {
            PartitionSet set = Objects.requireNonNullElseGet(old, BitSetPartitionSet::new);
            set.set(partitionIndex);
            return set;
        });
    }

    /** Returns local partitions for a table, or {@link PartitionSet#EMPTY_SET} if absent. */
    PartitionSet localPartitions(int tableId) {
        return localPartsByTableId.getOrDefault(tableId, PartitionSet.EMPTY_SET);
    }
}
