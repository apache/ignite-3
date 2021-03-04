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

import java.util.function.Predicate;
import org.apache.ignite.table.KVView;
import org.apache.ignite.table.RecordView;
import org.apache.ignite.table.BinaryRow;
import org.apache.ignite.table.storage.TableStorage;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.mapper.KeyMapper;
import org.apache.ignite.table.mapper.RecordMapper;
import org.apache.ignite.table.mapper.ValueMapper;

/**
 * Table view implementation provides functionality to access binary rows.
 */
public class TableViewImpl implements Table {
    /** Table. */
    private TableStorage table;

    /**
     * Constructor.
     *
     * @param table Table.
     */
    public TableViewImpl(TableStorage table) {
        this.table = table;
    }

    /** {@inheritDoc} */
    @Override public <R> RecordView<R> recordView(RecordMapper<R> recMapper) {
        return new RecordViewImpl<>(table, recMapper);
    }

    /** {@inheritDoc} */
    @Override public <K, V> KVView<K, V> kvView(KeyMapper<K> keyMapper, ValueMapper<V> valMapper) {
        return new KVViewImpl<>(table, keyMapper, valMapper);
    }

    /** {@inheritDoc} */
    @Override public BinaryRow get(BinaryRow keyRow) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Iterable<BinaryRow> find(Predicate<BinaryRow> template) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean upsert(BinaryRow row) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean insert(BinaryRow row) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public BinaryRow createSearchRow(Object... args) {
        BinaryRow row = null;

//        TableSchema schema = table.schemaManager().schema();
//        assert args.length == schema.keyColumns().length();

//        for (int i = 0; i < args.length; i++)
//            row.setColumn(i, args[i]);

        return row;
    }
}
