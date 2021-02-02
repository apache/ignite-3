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

package org.apache.ignite.internal.storage;

import org.apache.ignite.storage.KVView;
import org.apache.ignite.storage.RecordView;
import org.apache.ignite.storage.Row;
import org.apache.ignite.storage.TableStorage;
import org.apache.ignite.storage.Table;
import org.apache.ignite.storage.mapper.KeyMapper;
import org.apache.ignite.storage.mapper.RowMapper;
import org.apache.ignite.storage.mapper.ValueMapper;

public class TableViewImpl implements Table {
    private TableStorage table;

    public TableViewImpl(TableStorage table) {
        this.table = table;
    }

    @Override public <R> RecordView<R> recordView(RowMapper<R> rowMapper) {
        return new RecordViewImpl<>(table, rowMapper);
    }

    @Override public <K, V> KVView<K, V> kvView(KeyMapper<K> keyMapper, ValueMapper<V> valMapper) {
        return new KVViewImpl<>(table, keyMapper, valMapper);
    }

    @Override public Row get(Row keyRow) {
        return null;
    }

    @Override public Iterable<Row> find(Row template) {
        return null;
    }

    @Override public boolean upsert(Row row) {
        return false;
    }

    @Override public boolean insert(Row row) {
        return false;
    }

    @Override public Row createSearchRow(Object... args) {
        return null;
    }
}
