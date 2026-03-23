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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.table.TableViewInternal;

/**
 * Holds shared mutable state for table tracking, shared between {@link TableManager} and {@link TableZoneCoordinator}.
 *
 * <ul>
 *   <li>{@link #tables} — all registered tables by ID.</li>
 *   <li>{@link #startedTables} — tables that are fully started (partition resources prepared).</li>
 *   <li>{@link #localPartsByTableId} — local partition sets per table.</li>
 * </ul>
 */
class TableRegistry {
    /** All registered tables by ID. */
    private final Map<Integer, TableViewInternal> tables = new ConcurrentHashMap<>();

    /** Tables that are fully started (partition resources prepared). */
    private final Map<Integer, TableViewInternal> startedTables = new ConcurrentHashMap<>();

    /** Local partitions by table ID. */
    private final Map<Integer, PartitionSet> localPartsByTableId = new ConcurrentHashMap<>();

    Map<Integer, TableViewInternal> tables() {
        return tables;
    }

    Map<Integer, TableViewInternal> startedTables() {
        return startedTables;
    }

    Map<Integer, PartitionSet> localPartsByTableId() {
        return localPartsByTableId;
    }
}
