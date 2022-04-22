/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.MarshallerException;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerException;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.schema.marshaller.reflection.KvMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.NotNull;

/**
 * Table view implementation for binary objects.
 */
public class TableImpl implements Table {
    /** Internal table. */
    private final InternalTable tbl;

    /** Schema registry. */
    private final SchemaRegistry schemaReg;

    /**
     * Constructor.
     *
     * @param tbl       The table.
     * @param schemaReg Table schema registry.
     */
    public TableImpl(InternalTable tbl, SchemaRegistry schemaReg) {
        this.tbl = tbl;
        this.schemaReg = schemaReg;
    }

    /**
     * Gets a table id.
     *
     * @return Table id as UUID.
     */
    public @NotNull UUID tableId() {
        return tbl.tableId();
    }

    /** Returns an internal table instance this view represents. */
    public InternalTable internalTable() {
        return tbl;
    }

    /** {@inheritDoc} */
    @Override public @NotNull String name() {
        return tbl.name();
    }

    /**
     * Gets a schema view for the table.
     *
     * @return Schema view.
     */
    public SchemaRegistry schemaView() {
        return schemaReg;
    }

    /** {@inheritDoc} */
    @Override
    public <R> RecordView<R> recordView(Mapper<R> recMapper) {
        return new RecordViewImpl<>(tbl, schemaReg, recMapper);
    }

    /** {@inheritDoc} */
    @Override
    public RecordView<Tuple> recordView() {
        return new RecordBinaryViewImpl(tbl, schemaReg);
    }

    /** {@inheritDoc} */
    @Override
    public <K, V> KeyValueView<K, V> keyValueView(Mapper<K> keyMapper, Mapper<V> valMapper) {
        return new KeyValueViewImpl<>(tbl, schemaReg, keyMapper, valMapper);
    }

    /** {@inheritDoc} */
    @Override
    public KeyValueView<Tuple, Tuple> keyValueView() {
        return new KeyValueBinaryViewImpl(tbl, schemaReg);
    }

    /**
     * Returns a partition for a key tuple.
     *
     * @param key The tuple.
     * @return The partition.
     */
    public int partition(Tuple key) {
        Objects.requireNonNull(key);

        try {
            final Row keyRow = new TupleMarshallerImpl(schemaReg).marshalKey(key);

            return tbl.partition(keyRow);
        } catch (TupleMarshallerException e) {
            throw new IgniteInternalException(e);
        }
    }

    /**
     * Returns a partition for a key.
     *
     * @param key The key.
     * @param keyMapper Key mapper
     * @return The partition.
     */
    public <K> int partition(K key, Mapper<K> keyMapper) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(keyMapper);

        BinaryRowEx keyRow;
        var marshaller = new KvMarshallerImpl<>(schemaReg.schema(), keyMapper, keyMapper);
        try {
            keyRow = marshaller.marshal(key);
        } catch (MarshallerException e) {
            throw new IgniteInternalException("Cannot marshal key", e);
        }

        return tbl.partition(keyRow);
    }

    /**
     * Returns cluster node that is the leader of the corresponding partition group or throws an exception if
     * it cannot be found.
     *
     * @param partition partition number
     * @return leader node of the partition group corresponding to the partition
     */
    public ClusterNode leaderAssignment(int partition) {
        return tbl.leaderAssignment(partition);
    }
}
