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

package org.apache.ignite.internal.table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.apache.ignite.internal.marshaller.MarshallerException;
import org.apache.ignite.internal.marshaller.MarshallersProvider;
import org.apache.ignite.internal.marshaller.ReflectionMarshallersProvider;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.ColumnsExtractor;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerException;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.schema.marshaller.reflection.KvMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.storage.index.StorageHashIndexDescriptor;
import org.apache.ignite.internal.storage.index.StorageSortedIndexDescriptor;
import org.apache.ignite.internal.table.IndexWrapper.HashIndexWrapper;
import org.apache.ignite.internal.table.IndexWrapper.SortedIndexWrapper;
import org.apache.ignite.internal.table.distributed.IndexLocker;
import org.apache.ignite.internal.table.distributed.PartitionSet;
import org.apache.ignite.internal.table.distributed.TableIndexStoragesSupplier;
import org.apache.ignite.internal.table.distributed.TableSchemaAwareIndexStorage;
import org.apache.ignite.internal.table.distributed.schema.SchemaVersions;
import org.apache.ignite.internal.table.partition.HashPartitionManagerImpl;
import org.apache.ignite.internal.tx.LockManager;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.sql.IgniteSql;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.partition.HashPartition;
import org.apache.ignite.table.partition.PartitionManager;
import org.jetbrains.annotations.TestOnly;

/**
 * Table view implementation for binary objects.
 */
public class TableImpl implements TableViewInternal {
    /** Internal table. */
    private final InternalTable tbl;

    private final LockManager lockManager;

    private final SchemaVersions schemaVersions;

    /** Ignite SQL facade. */
    private final IgniteSql sql;

    /** Schema registry. Should be set either in constructor or via {@link #schemaView(SchemaRegistry)} before start of using the table. */
    private volatile SchemaRegistry schemaReg;

    private final Map<Integer, IndexWrapper> indexWrapperById = new ConcurrentHashMap<>();

    private final MarshallersProvider marshallers;

    private final int pkId;

    /**
     * Constructor.
     *
     * @param tbl The table.
     * @param lockManager Lock manager.
     * @param schemaVersions Schema versions access.
     * @param marshallers Marshallers provider.
     * @param sql Ignite SQL facade.
     * @param pkId ID of a primary index.
     */
    public TableImpl(
            InternalTable tbl,
            LockManager lockManager,
            SchemaVersions schemaVersions,
            MarshallersProvider marshallers,
            IgniteSql sql,
            int pkId
    ) {
        this.tbl = tbl;
        this.lockManager = lockManager;
        this.schemaVersions = schemaVersions;
        this.marshallers = marshallers;
        this.sql = sql;
        this.pkId = pkId;
    }

    /**
     * Constructor.
     *
     * @param tbl The table.
     * @param schemaReg Table schema registry.
     * @param lockManager Lock manager.
     * @param schemaVersions Schema versions access.
     * @param sql Ignite SQL facade.
     * @param pkId ID of a primary index.
     */
    @TestOnly
    public TableImpl(
            InternalTable tbl,
            SchemaRegistry schemaReg,
            LockManager lockManager,
            SchemaVersions schemaVersions,
            IgniteSql sql,
            int pkId
    ) {
        this(tbl, lockManager, schemaVersions, new ReflectionMarshallersProvider(), sql, pkId);

        this.schemaReg = schemaReg;
    }

    @Override
    public int tableId() {
        return tbl.tableId();
    }

    @Override
    public int pkId() {
        return pkId;
    }

    @Override
    public InternalTable internalTable() {
        return tbl;
    }

    @Override
    public PartitionManager<HashPartition> partitionManager() {
        return new HashPartitionManagerImpl(tbl, schemaReg, marshallers);
    }

    @Override public String name() {
        return tbl.name();
    }

    // TODO: revisit this approach, see https://issues.apache.org/jira/browse/IGNITE-21235.
    public void name(String newName) {
        tbl.name(newName);
    }

    @Override
    public SchemaRegistry schemaView() {
        return schemaReg;
    }

    @Override
    public void schemaView(SchemaRegistry schemaReg) {
        assert this.schemaReg == null : "Schema registry is already set [tableName=" + name() + ']';

        Objects.requireNonNull(schemaReg, "Schema registry must not be null [tableName=" + name() + ']');

        this.schemaReg = schemaReg;
    }

    @Override
    public <R> RecordView<R> recordView(Mapper<R> recMapper) {
        return new RecordViewImpl<>(tbl, schemaReg, schemaVersions, sql, marshallers, recMapper);
    }

    @Override
    public RecordView<Tuple> recordView() {
        return new RecordBinaryViewImpl(tbl, schemaReg, schemaVersions, sql, marshallers);
    }

    @Override
    public <K, V> KeyValueView<K, V> keyValueView(Mapper<K> keyMapper, Mapper<V> valMapper) {
        return new KeyValueViewImpl<>(tbl, schemaReg, schemaVersions, sql, marshallers, keyMapper, valMapper);
    }

    @Override
    public KeyValueView<Tuple, Tuple> keyValueView() {
        return new KeyValueBinaryViewImpl(tbl, schemaReg, schemaVersions, sql, marshallers);
    }

    @Override
    public int partition(Tuple key) {
        Objects.requireNonNull(key);

        try {
            // Taking latest schema version for marshaller here because it's only used to calculate colocation hash, and colocation
            // columns never change (so they are the same for all schema versions of the table),
            Row keyRow = new TupleMarshallerImpl(schemaReg.lastKnownSchema()).marshalKey(key);

            return tbl.partition(keyRow);
        } catch (TupleMarshallerException e) {
            throw new org.apache.ignite.lang.MarshallerException(e);
        }
    }

    @Override
    public <K> int partition(K key, Mapper<K> keyMapper) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(keyMapper);

        BinaryRowEx keyRow;
        var marshaller = new KvMarshallerImpl<>(schemaReg.lastKnownSchema(), marshallers, keyMapper, keyMapper);
        try {
            keyRow = marshaller.marshal(key);
        } catch (MarshallerException e) {
            throw new org.apache.ignite.lang.MarshallerException(e);
        }

        return tbl.partition(keyRow);
    }

    @Override
    public ClusterNode leaderAssignment(int partition) {
        return tbl.tableRaftService().leaderAssignment(partition);
    }

    /** Returns a supplier of index storage wrapper factories for given partition. */
    public TableIndexStoragesSupplier indexStorageAdapters(int partId) {
        return () -> {
            List<IndexWrapper> factories = new ArrayList<>(indexWrapperById.values());

            Map<Integer, TableSchemaAwareIndexStorage> adapters = new HashMap<>();

            for (IndexWrapper factory : factories) {
                TableSchemaAwareIndexStorage storage = factory.getStorage(partId);
                adapters.put(storage.id(), storage);
            }

            return adapters;
        };
    }

    /** Returns a supplier of index locker factories for given partition. */
    public Supplier<Map<Integer, IndexLocker>> indexesLockers(int partId) {
        return () -> {
            List<IndexWrapper> factories = new ArrayList<>(indexWrapperById.values());

            Map<Integer, IndexLocker> lockers = new HashMap<>(factories.size());

            for (IndexWrapper factory : factories) {
                IndexLocker locker = factory.getLocker(partId);
                lockers.put(locker.id(), locker);
            }

            return lockers;
        };
    }

    @Override
    public void registerHashIndex(
            StorageHashIndexDescriptor indexDescriptor,
            boolean unique,
            ColumnsExtractor searchRowResolver,
            PartitionSet partitions
    ) {
        int indexId = indexDescriptor.id();

        // TODO: https://issues.apache.org/jira/browse/IGNITE-19112 Create storages once.
        partitions.stream().forEach(partitionId -> {
            tbl.storage().getOrCreateHashIndex(partitionId, indexDescriptor);
        });

        indexWrapperById.put(indexId, new HashIndexWrapper(tbl, lockManager, indexId, searchRowResolver, unique));
    }

    @Override
    public void registerSortedIndex(
            StorageSortedIndexDescriptor indexDescriptor,
            ColumnsExtractor searchRowResolver,
            PartitionSet partitions
    ) {
        int indexId = indexDescriptor.id();

        // TODO: https://issues.apache.org/jira/browse/IGNITE-19112 Create storages once.
        partitions.stream().forEach(partitionId -> {
            tbl.storage().getOrCreateSortedIndex(partitionId, indexDescriptor);
        });

        indexWrapperById.put(indexId, new SortedIndexWrapper(tbl, lockManager, indexId, searchRowResolver));
    }

    @Override
    public void unregisterIndex(int indexId) {
        indexWrapperById.remove(indexId);

        tbl.storage().destroyIndex(indexId);
    }
}
