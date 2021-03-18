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

package org.apache.ignite.table;

import org.apache.ignite.table.mapper.KeyMapper;
import org.apache.ignite.table.mapper.Mappers;
import org.apache.ignite.table.mapper.RecordMapper;
import org.apache.ignite.table.mapper.ValueMapper;

/**
 * Table view interface provides synchronous and asynchronous methods to access table data
 * regarding binary object concept.
 *
 * @apiNote Some methods require a record with the only key fields set. This is not mandatory requirement
 * and value fields will be just ignored.
 */
public interface Table extends TableView<ColSpan> {
    /**
     * Creates record view of table for record class mapper provided.
     *
     * @param recMapper Record class mapper.
     * @return Table record view.
     */
    <R> RecordView<R> recordView(RecordMapper<R> recMapper);

    /**
     * Creates key-value view of table for key-value class mappers provided.
     *
     * @param keyMapper Key class mapper.
     * @param valMapper Value class mapper.
     * @return Table key-value view.
     */
    <K, V> KeyValueView<K, V> kvView(KeyMapper<K> keyMapper, ValueMapper<V> valMapper);

    /**
     * Creates key-value like projection over table implementing BinaryObject concept.
     *
     * @return Table key-value view.
     */
    KeyValueBinaryView kvView();

    /**
     * Creates record view of table for record class provided.
     *
     * @param recCls Record class.
     * @return Table record view.
     */
    default <R> RecordView<R> recordView(Class<R> recCls) {
        return recordView(Mappers.ofRowClass(recCls));
    }

    /**
     * Creates key-value view of table for key and value classes provided.
     *
     * @param keyCls Key class.
     * @param valCls Value class.
     * @return Table key-value view.
     */
    default <K, V> KeyValueView<K, V> kvView(Class<K> keyCls, Class<V> valCls) {
        return kvView(Mappers.ofKeyClass(keyCls), Mappers.ofValueClass(valCls));
    }

    /**
     * Factory method shortcut.
     *
     * @return Binary builder.
     */
    ColSpanBuilder binaryBuilder();
}
