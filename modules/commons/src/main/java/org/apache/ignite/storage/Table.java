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

package org.apache.ignite.storage;

import org.apache.ignite.storage.mapper.KeyMapper;
import org.apache.ignite.storage.mapper.Mappers;
import org.apache.ignite.storage.mapper.RowMapper;
import org.apache.ignite.storage.mapper.ValueMapper;

/**
 *
 */
public interface Table {
    public <R> TableView<R> tableView(RowMapper<R> rowMapper);

    public <K, V> KVView<K, V> kvView(KeyMapper<K> keyMapper, ValueMapper<V> valMapper);

    public default <R> TableView<R> tableView(Class<R> rowClass) {
        return tableView(Mappers.ofRowClass(rowClass));
    }

    public default <K, V> KVView<K, V> kvView(Class<K> kCls, Class<V> vCls) {
        return kvView(Mappers.ofKeyClass(kCls), Mappers.ofValueClass(vCls));
    }

    public Row get(Row keyRow);

    public Iterable<Row> find(Row template);

    public boolean upsert(Row row);

    public boolean insert(Row row);

    Row createSearchRow(Object... args);
}
