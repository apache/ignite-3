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

package org.apache.ignite.internal.idx;

import java.util.BitSet;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.storage.index.SortedIndexColumnDescriptor;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Internal index manager facade provides low-level methods for indexes operations.
 */
public class InternalSortedIndexImpl implements InternalSortedIndex {
    private final IgniteUuid id;

    private final String name;

    private final SortedIndexStorage store;

    private final TableImpl tbl;

    /**
     * Create sorted index.
     */
    public InternalSortedIndexImpl(IgniteUuid id, String name, SortedIndexStorage store, TableImpl tbl) {
        this.id = id;
        this.name = name;
        this.store = store;
        this.tbl = tbl;
    }

    @Override
    public IgniteUuid id() {
        return id;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String tableName() {
        return tbl.name();
    }

    @Override
    public List<Column> columns() {
        return store.indexDescriptor().columns().stream()
                .map(SortedIndexColumnDescriptor::column)
                .collect(Collectors.toList());
    }

    @Override
    public Cursor<Row> scan(Row low, Row up, byte scanBoundMask, BitSet proj) {
        return null;
    }
}
