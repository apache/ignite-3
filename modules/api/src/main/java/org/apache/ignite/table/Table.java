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

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.table.binary.Row;
import org.apache.ignite.table.binary.RowBuilder;
import org.apache.ignite.table.mapper.KeyMapper;
import org.apache.ignite.table.mapper.Mappers;
import org.apache.ignite.table.mapper.RecordMapper;
import org.apache.ignite.table.mapper.ValueMapper;

/**
 * Table binary view.
 */
public interface Table extends TableView<Row> {
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
    <K, V> KVView<K, V> kvView(KeyMapper<K> keyMapper, ValueMapper<V> valMapper);

    /**
     * Creates key-value like projection over table implementing BinaryObject concept.
     *
     * @return Table key-value view.
     */
    KVBinaryView kvView();

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
    default <K, V> KVView<K, V> kvView(Class<K> keyCls, Class<V> valCls) {
        return kvView(Mappers.ofKeyClass(keyCls), Mappers.ofValueClass(valCls));
    }

    /**
     * Invokes an InvokeProcessor against the associated row.
     *
     * @param keyRow Row with key columns set.
     * @return Results of the processing.
     */
     <T extends Serializable> T invoke(Row keyRow, BinaryInvokeProcessor<T> proc);

    /**
     * Invokes an InvokeProcessor against the associated rows.
     *
     * @param keyRows Ordered collection of rows with key columns set.
     * @return Results of the processing.
     */
     <T extends Serializable> Map<Row, T> invokeAll(Collection<Row> keyRows, BinaryInvokeProcessor<T> proc);

    /**
     * Creates builder for BinaryRow.
     *
     * @return BinaryRow builder for table.
     */
    RowBuilder rowBuilder();
}
