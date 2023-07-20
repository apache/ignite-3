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
    public CompletableFuture<ExecutableTable> getTable(int schemaVersion, int tableId, TableDescriptor tableDescriptor) {
        //TODO: SchemaVersion must a part of cache key, or cache InternalTable only.
        // return tableCache.computeIfAbsent(tableId, (k) -> loadTable(schemaVersion, k, tableDescriptor));
        return loadTable(schemaVersion, tableId, tableDescriptor);
    }

    /** {@inheritDoc} */
    @Override
    public void onSchemaUpdated() {
        tableCache.clear();
    }

    private CompletableFuture<ExecutableTable> loadTable(int schemaVersion, int tableId, TableDescriptor tableDescriptor) {
        return tableManager.tableAsync(tableId)
                .thenApply(table -> {
                    InternalTable internalTable = table.internalTable();
                    SchemaRegistry schemaRegistry = schemaManager.schemaRegistry(tableId);

                    SchemaDescriptor schemaDescriptor = schemaRegistry.schema(schemaVersion);
                    TableRowConverter rowConverter = new TableRowConverterImpl(schemaRegistry, schemaDescriptor, tableDescriptor);
                    ScannableTable scannableTable = new ScannableTableImpl(internalTable, rowConverter, tableDescriptor);

                    UpdatableTableImpl updatableTable = new UpdatableTableImpl(tableId, tableDescriptor, internalTable.partitions(),
                            replicaService, clock, rowConverter, schemaDescriptor);

                    return new ExecutableTableImpl(scannableTable, updatableTable);
                });
    }

    private static final class ExecutableTableImpl implements ExecutableTable {

        private final ScannableTable scannableTable;

        private final UpdatableTable updatableTable;

        private ExecutableTableImpl(ScannableTable scannableTable, UpdatableTable updatableTable) {
            this.scannableTable = scannableTable;
            this.updatableTable = updatableTable;
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
    }
}
