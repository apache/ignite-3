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

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerException;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.table.KeyValueView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Table view implementation for binary objects.
 */
public class TableImpl implements Table, StorageRowListener {
    /** Internal table. */
    private final InternalTable tbl;

    /** Schema registry. */
    private final SchemaRegistry schemaReg;

    private final Set<StorageRowListener> rowLstns = Collections.newSetFromMap(new ConcurrentHashMap<>());

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
     * Returns a partition for a tuple.
     *
     * @param t The tuple.
     * @return The partition.
     */
    @TestOnly
    public int partition(Tuple t) {
        Objects.requireNonNull(t);

        try {
            final Row keyRow = new TupleMarshallerImpl(schemaReg).marshalKey(t);

            return tbl.partition(keyRow);
        } catch (TupleMarshallerException e) {
            throw new IgniteInternalException(e);
        }
    }

    public void addRowListener(StorageRowListener lsnr) {
        rowLstns.add(lsnr);
    }

    public void removeRowListener(StorageRowListener lsnr) {
        rowLstns.remove(lsnr);
    }

    @Override
    public void onUpdate(@Nullable BinaryRow oldRow, BinaryRow newRow, int partId) {
        rowLstns.forEach(l -> l.onUpdate(oldRow, newRow, partId));
    }

    @Override
    public void onRemove(BinaryRow row, int partId) {
        rowLstns.forEach(l -> l.onRemove(row, partId));
    }
}
