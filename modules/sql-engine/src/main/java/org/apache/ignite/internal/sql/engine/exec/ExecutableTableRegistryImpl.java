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

import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.sql.engine.metadata.ColocationGroup;
import org.apache.ignite.internal.sql.engine.metadata.NodeWithTerm;
import org.apache.ignite.internal.sql.engine.schema.SchemaUpdateListener;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.distributed.TableManager;


/**
 * Implementation of {@link ExecutableTableRegistry}.
 */
public class ExecutableTableRegistryImpl implements ExecutableTableRegistry, SchemaUpdateListener {

    private final TableManager tableManager;

    private final SchemaManager schemaManager;

    private final ReplicaService replicaService;

    private final HybridClock clock;

    final ConcurrentMap<Integer, CompletableFuture<ExecutableTable>> tableCache;

    /** Constructor. */
    public ExecutableTableRegistryImpl(TableManager tableManager, SchemaManager schemaManager,
            ReplicaService replicaService, HybridClock clock, int cacheSize) {

        this.tableManager = tableManager;
        this.schemaManager = schemaManager;
        this.replicaService = replicaService;
        this.clock = clock;
        this.tableCache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .<Integer, ExecutableTable>buildAsync().asMap();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<ExecutableTable> getTable(int tableId, TableDescriptor tableDescriptor) {
        return tableCache.computeIfAbsent(tableId, (k) -> loadTable(k, tableDescriptor));
    }

    // TODO IGNITE-19499: Drop this temporal method to get table by name.
    @Override
    public CompletableFuture<ExecutableTable> getTable(int tableId, String tableName, TableDescriptor tableDescriptor) {
        return tableManager.tableAsyncInternal(tableName.toUpperCase())
                .thenApply(table -> {
                    InternalTable internalTable = table.internalTable();
                    SchemaRegistry schemaRegistry = schemaManager.schemaRegistry(table.tableId());
                    SchemaDescriptor schemaDescriptor = schemaRegistry.schema();
                    TableRowConverter rowConverter = new TableRowConverterImpl(schemaRegistry, schemaDescriptor, tableDescriptor);
                    ScannableTable scannableTable = new ScannableTableImpl(internalTable, rowConverter, tableDescriptor);

                    UpdatableTableImpl updatableTable = new UpdatableTableImpl(table.tableId(), tableDescriptor, internalTable.partitions(),
                            replicaService, clock, rowConverter, schemaDescriptor);

                    return new ExecutableTableImpl(internalTable, scannableTable, updatableTable);
                });
    }

    /** {@inheritDoc} */
    @Override
    public void onSchemaUpdated() {
        tableCache.clear();
    }

    private CompletableFuture<ExecutableTable> loadTable(int tableId, TableDescriptor tableDescriptor) {
        CompletableFuture<Map.Entry<InternalTable, SchemaRegistry>> f = tableManager.tableAsync(tableId)
                .thenApply(table -> {
                    InternalTable internalTable = table.internalTable();
                    SchemaRegistry schemaRegistry = schemaManager.schemaRegistry(tableId);
                    return Map.entry(internalTable, schemaRegistry);
                });

        return f.thenApply((table) -> {
            SchemaRegistry schemaRegistry = table.getValue();
            SchemaDescriptor schemaDescriptor = schemaRegistry.schema();
            TableRowConverter rowConverter = new TableRowConverterImpl(schemaRegistry, schemaDescriptor, tableDescriptor);
            InternalTable internalTable = table.getKey();
            ScannableTable scannableTable = new ScannableTableImpl(internalTable, rowConverter, tableDescriptor);

            UpdatableTableImpl updatableTable = new UpdatableTableImpl(tableId, tableDescriptor, internalTable.partitions(),
                    replicaService, clock, rowConverter, schemaDescriptor);

            return new ExecutableTableImpl(internalTable, scannableTable, updatableTable);
        });
    }

    private static final class ExecutableTableImpl implements ExecutableTable {

        private final InternalTable internalTable;

        private final ScannableTable scannableTable;

        private final UpdatableTable updatableTable;

        private ExecutableTableImpl(InternalTable internalTable, ScannableTable scannableTable, UpdatableTable updatableTable) {
            this.internalTable = internalTable;
            this.scannableTable = scannableTable;
            this.updatableTable = updatableTable;
        }

        // TODO IGNITE-19499: Drop this.
        @Deprecated(forRemoval = true)
        @Override
        public InternalTable internalTable() {
            return internalTable;
        }

        /** {@inheritDoc} */
        @Override
        public ScannableTable scannableTable() {
            return scannableTable;
        }

        /** {@inheritDoc} */
        @Override
        public UpdatableTable updatableTable() {
            return updatableTable;
        }

        /** {@inheritDoc} */
        @Override
        public CompletableFuture<ColocationGroup> fetchColocationGroup() {
            return internalTable.primaryReplicas().thenApply(rs -> {
                List<List<NodeWithTerm>> assignments = rs.stream()
                        .map(primaryReplica -> new NodeWithTerm(primaryReplica.node().name(), primaryReplica.term()))
                        .map(Collections::singletonList)
                        .collect(Collectors.toList());

                return ColocationGroup.forAssignments(assignments);
            });
        }
    }
}
