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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.sql.engine.metadata.ColocationGroup;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;

/**
 * Provides access to resolved dependencies.
 */
public class ResolvedDependencies {

    private final Map<Integer, ExecutableTable> tableMap;

    /** Constructor. */
    public ResolvedDependencies(Map<Integer, ExecutableTable> tableMap) {
        this.tableMap = tableMap;
    }

    /**
     * Returns a table with the given id.
     */
    public ScannableTable scannableTable(int tableId) {
        ExecutableTable executableTable = getTable(tableId);
        return executableTable.scannableTable();
    }

    /**
     * Returns updatable table with the given id.
     */
    public UpdatableTable updatableTable(int tableId) {
        ExecutableTable executableTable = getTable(tableId);
        return executableTable.updatableTable();
    }

    /**
     * Retrieves colocation group for a table with the given id.
     */
    public CompletableFuture<ColocationGroup> fetchColocationGroup(int tableId) {
        ExecutableTable executableTable = getTable(tableId);
        return executableTable.fetchColocationGroup();
    }

    /**
     * Returns a descriptor for a table with the given id.
     */
    public TableDescriptor tableDescriptor(int tableId) {
        ExecutableTable executableTable = getTable(tableId);
        return executableTable.tableDescriptor();
    }

    public Set<Integer> tableIds() {
        return tableMap.keySet();
    }

    private ExecutableTable getTable(int tableId) {
        ExecutableTable executableTable = tableMap.get(tableId);
        assert executableTable != null : "ExecutableTable does not exist: " + tableId;
        return executableTable;
    }
}
