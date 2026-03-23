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

import java.util.Objects;
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
import org.apache.ignite.internal.sql.engine.util.cache.Cache;
import org.apache.ignite.internal.sql.engine.util.cache.CacheFactory;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.table.TableViewInternal;
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
    final Cache<CacheKey, ExecutableTable> tableCache;

    /** Constructor. */
    public ExecutableTableRegistryImpl(
            TableManager tableManager,
            SchemaManager schemaManager,
            SqlSchemaManager sqlSchemaManager,
            ReplicaService replicaService,
            ClockService clockService,
            int cacheSize,
            CacheFactory cacheFactory
    ) {

        this.sqlSchemaManager = sqlSchemaManager;
        this.tableManager = tableManager;
        this.schemaManager = schemaManager;
        this.replicaService = replicaService;
        this.clockService = clockService;
        this.tableCache = cacheFactory.create(cacheSize);
    }

    /** {@inheritDoc} */
    @Override
    public ExecutableTable getTable(int catalogVersion, int tableId) {
        IgniteTable sqlTable = sqlSchemaManager.table(catalogVersion, tableId);

        return tableCache.get(cacheKey(tableId, sqlTable.timestamp()), (k) -> loadTable(sqlTable));
    }

    private ExecutableTable loadTable(IgniteTable sqlTable) {
        TableViewInternal table = tableManager.cachedTable(sqlTable.id());

        assert table != null : "Table not found: tableId=" + sqlTable.id();

        TableDescriptor tableDescriptor = sqlTable.descriptor();

        SchemaRegistry schemaRegistry = schemaManager.schemaRegistry(sqlTable.id());
        SchemaDescriptor schemaDescriptor = schemaRegistry.schema(sqlTable.version());
        TableRowConverterFactory converterFactory = new TableRowConverterFactoryImpl(
                tableDescriptor, schemaRegistry, schemaDescriptor
        );

        InternalTable internalTable = table.internalTable();
        ScannableTable scannableTable = new ScannableTableImpl(internalTable, converterFactory);
        TableRowConverter rowConverter = converterFactory.create(null);

        UpdatableTableImpl updatableTable = new UpdatableTableImpl(
                tableDescriptor,
                internalTable,
                replicaService,
                clockService,
                rowConverter
        );

        return new ExecutableTableImpl(scannableTable, updatableTable, sqlTable.partitionCalculator());
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

    private static CacheKey cacheKey(int tableId, long timestamp) {
        return new CacheKey(tableId, timestamp);
    }

    private static class CacheKey {
        private final int tableId;
        private final long timestamp;

        CacheKey(int tableId, long timestamp) {
            this.tableId = tableId;
            this.timestamp = timestamp;
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
            return timestamp == cacheKey.timestamp && tableId == cacheKey.tableId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(timestamp, tableId);
        }
    }

}
