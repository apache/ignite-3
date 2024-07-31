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
import java.util.function.Supplier;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.SchemaManager;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.PartitionCalculator;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.distributed.TableManager;

/**
 * Implementation of {@link ExecutableTableRegistry}.
 */
public class ExecutableTableRegistryImpl implements ExecutableTableRegistry {

    private final TableManager tableManager;

    private final SqlSchemaManager sqlSchemaManager;

    private final SchemaManager schemaManager;

    private final ReplicaService replicaService;

    private final ClockService clockService;

    /** Executable tables cache. */
    final ConcurrentMap<CacheKey, CompletableFuture<ExecutableTable>> tableCache;

    /** Constructor. */
    public ExecutableTableRegistryImpl(
            TableManager tableManager,
            SchemaManager schemaManager,
            SqlSchemaManager sqlSchemaManager,
            ReplicaService replicaService,
            ClockService clockService,
            int cacheSize
    ) {

        this.sqlSchemaManager = sqlSchemaManager;
        this.tableManager = tableManager;
        this.schemaManager = schemaManager;
        this.replicaService = replicaService;
        this.clockService = clockService;
        this.tableCache = Caffeine.newBuilder()
                .maximumSize(cacheSize)
                .<CacheKey, ExecutableTable>buildAsync().asMap();
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<ExecutableTable> getTable(int catalogVersion, int tableId) {
        IgniteTable sqlTable = sqlSchemaManager.table(catalogVersion, tableId);

        return tableCache.computeIfAbsent(cacheKey(tableId, sqlTable.version()), (k) -> loadTable(sqlTable));
    }

    // TODO https://issues.apache.org/jira/browse/IGNITE-21584 Remove future.
    private CompletableFuture<ExecutableTable> loadTable(IgniteTable sqlTable) {
        return CompletableFuture.completedFuture(tableManager.cachedTable(sqlTable.id()))
                .thenApply((table) -> {
                    TableDescriptor tableDescriptor = sqlTable.descriptor();

                    SchemaRegistry schemaRegistry = schemaManager.schemaRegistry(sqlTable.id());
                    SchemaDescriptor schemaDescriptor = schemaRegistry.schema(sqlTable.version());
                    TableRowConverterFactory converterFactory = new TableRowConverterFactoryImpl(
                            tableDescriptor, schemaRegistry, schemaDescriptor
                    );

                    InternalTable internalTable = table.internalTable();
                    ScannableTable scannableTable = new ScannableTableImpl(internalTable, converterFactory);
                    TableRowConverter rowConverter = converterFactory.create(null);

                    UpdatableTableImpl updatableTable = new UpdatableTableImpl(sqlTable.id(), tableDescriptor, internalTable.partitions(),
                            internalTable, replicaService, clockService, rowConverter);

                    return new ExecutableTableImpl(scannableTable, updatableTable, sqlTable.partitionCalculator());
                });
    }

    private static final class ExecutableTableImpl implements ExecutableTable {
        private final ScannableTable scannableTable;

        private final UpdatableTable updatableTable;

        private final Supplier<PartitionCalculator> partitionCalculator;

        private ExecutableTableImpl(
                ScannableTable scannableTable,
                UpdatableTable updatableTable,
                Supplier<PartitionCalculator> partitionCalculator
        ) {
            this.scannableTable = scannableTable;
            this.updatableTable = updatableTable;
            this.partitionCalculator = partitionCalculator;
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

        /** {@inheritDoc} */
        @Override
        public Supplier<PartitionCalculator> partitionCalculator() {
            return partitionCalculator;
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
