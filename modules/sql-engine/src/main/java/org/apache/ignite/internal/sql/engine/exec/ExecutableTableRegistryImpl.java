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
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.CatalogSchemaManager;
import org.apache.ignite.internal.schema.SchemaDescriptor;
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

    private final CatalogSchemaManager schemaManager;

    private final ReplicaService replicaService;

    private final HybridClock clock;

    /** Executable tables cache. */
    final ConcurrentMap<CacheKey, CompletableFuture<ExecutableTable>> tableCache;

    /** Constructor. */
    public ExecutableTableRegistryImpl(TableManager tableManager, CatalogSchemaManager schemaManager,
            ReplicaService replicaService, HybridClock clock, int cacheSize) {

        this.tableManager = tableManager;
        this.schemaManager = schemaManager;
        this.replicaService = replicaService;
        this.clock = clock;
        this.tableCache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .<CacheKey, ExecutableTable>buildAsync().asMap();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<ExecutableTable> getTable(int tableId, int tableVersion, TableDescriptor tableDescriptor) {
        return tableCache.computeIfAbsent(cacheKey(tableId, tableVersion), (k) -> loadTable(tableId, tableVersion, tableDescriptor));
    }

    /** {@inheritDoc} */
    @Override
    public void onSchemaUpdated() {
        tableCache.clear();
    }

    private CompletableFuture<ExecutableTable> loadTable(int tableId, int tableVersion, TableDescriptor tableDescriptor) {
        return tableManager.tableAsync(tableId)
                .thenApply((table) -> {
                    SchemaRegistry schemaRegistry = schemaManager.schemaRegistry(tableId);
                    SchemaDescriptor schemaDescriptor = schemaRegistry.schema(tableVersion);
                    TableRowConverterFactory converterFactory = requiredColumns -> new TableRowConverterImpl(
                            schemaRegistry, schemaDescriptor, tableDescriptor, requiredColumns
                    );

                    InternalTable internalTable = table.internalTable();
                    ScannableTable scannableTable = new ScannableTableImpl(internalTable, converterFactory, tableDescriptor);

                    UpdatableTableImpl updatableTable = new UpdatableTableImpl(tableId, tableDescriptor, internalTable.partitions(),
                            replicaService, clock, converterFactory.create(null), schemaDescriptor);

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

        /** {@inheritDoc} */
        @Override
        public TableDescriptor tableDescriptor() {
            return updatableTable.descriptor();
        }
    }

    private static CacheKey cacheKey(int tableId, int version) {
        return new CacheKey(tableId, version);
    }

    private static class CacheKey {
        private final int tableId;
        private final int tableVersion;

        CacheKey(int tableId, int tableVersion) {
            this.tableId = tableId;
            this.tableVersion = tableVersion;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return tableVersion == cacheKey.tableVersion && tableId == cacheKey.tableId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableVersion, tableId);
        }
    }
}
