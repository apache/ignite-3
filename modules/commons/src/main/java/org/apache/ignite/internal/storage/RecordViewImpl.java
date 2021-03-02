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

import org.apache.ignite.internal.schema.marshaller.Marshaller;
import org.apache.ignite.storage.RecordView;
import org.apache.ignite.storage.TableStorage;
import org.apache.ignite.storage.mapper.RowMapper;

public class RecordViewImpl<R> implements RecordView<R> {

    private final TableStorage table;
    private final RowMapper<R> rowMapper;
    Marshaller marsh;

    public RecordViewImpl(TableStorage table, RowMapper<R> mapper) {
        this.table = table;
        rowMapper = mapper;
    }

    @Override public R get(R record) {
        //        marsh = table.schemaManager().marshaller();

        TableRow kRow = marsh.toKeyRow(record);

        TableRow tRow = table.get(kRow);

        return marsh.unmarshallToRecord(record, tRow);
    }

    @Override public boolean upsert(R row) {
        return false;
    }

    @Override public boolean insert(R row) {
        return false;
    }
}
