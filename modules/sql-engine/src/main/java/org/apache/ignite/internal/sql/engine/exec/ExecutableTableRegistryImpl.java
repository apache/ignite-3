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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaRegistry;
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
            InternalTable internalTable = table.getKey();
            SchemaRegistry schemaRegistry = table.getValue();
            SchemaDescriptor schemaDescriptor = schemaRegistry.schema();
            TableRowConverter rowConverter = new TableRowConverterImpl(schemaRegistry, schemaDescriptor, tableDescriptor);

            UpdatableTableImpl updatableTable = new UpdatableTableImpl(tableId, tableDescriptor, internalTable.partitions(),
                    replicaService, clock, rowConverter, schemaDescriptor);

            return new ExecutableTableImpl(internalTable, updatableTable, rowConverter);
        });
    }

    private static final class ExecutableTableImpl implements ExecutableTable {

        private final InternalTable table;

        private final UpdateableTable updateableTable;

        private final TableRowConverter rowConverter;

        private ExecutableTableImpl(InternalTable table, UpdateableTable updateableTable, TableRowConverter rowConverter) {
            this.table = table;
            this.updateableTable = updateableTable;
            this.rowConverter = rowConverter;
        }

        /** {@inheritDoc} */
        @Override
        public InternalTable table() {
            return table;
        }

        /** {@inheritDoc} */
        @Override
        public UpdateableTable updates() {
            return updateableTable;
        }

        /** {@inheritDoc} */
        @Override
        public TableRowConverter rowConverter() {
            return rowConverter;
        }
    }
}
