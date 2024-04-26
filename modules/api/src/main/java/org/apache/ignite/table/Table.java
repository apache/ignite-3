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

package org.apache.ignite.table;

import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.partition.HashPartition;
import org.apache.ignite.table.partition.PartitionManager;

/**
 * Table provides the different views (key-value vs record) and approaches (mapped-object vs binary) to access the data.
 *
 * <p>Binary table views might be useful in the various scenarios, including when the user key-value classes 
 * are not in classpath and/or when deserialization of the entire table record is undesirable for performance reasons.
 *
 * @see RecordView
 * @see KeyValueView
 * @see org.apache.ignite.table.mapper.Mapper
 */
public interface Table {
    /**
     * Gets a name of a table.
     *
     * @return Table name.
     */
    String name();

    /**
     * Gets a partition manager of a table.
     *
     * @return Partition manager.
     */
    PartitionManager<HashPartition> partitionManager();

    /**
     * Creates a record view of a table for the record class mapper provided.
     *
     * @param recMapper Record class mapper.
     * @param <R>       Record type.
     * @return Table record view.
     */
    <R> RecordView<R> recordView(Mapper<R> recMapper);

    /**
     * Creates a record view of a table for the binary object concept.
     *
     * @return Table record view.
     */
    RecordView<Tuple> recordView();

    /**
     * Creates a record view of a table for the record class provided.
     *
     * @param recCls Record class.
     * @param <R>    Record type.
     * @return Table record view.
     */
    default <R> RecordView<R> recordView(Class<R> recCls) {
        return recordView(Mapper.of(recCls));
    }

    /**
     * Creates a key-value view of a table for the key-value class mappers provided.
     *
     * @param keyMapper Key class mapper.
     * @param valMapper Value class mapper.
     * @param <K>       Key type.
     * @param <V>       Value type.
     * @return Table key-value view.
     */
    <K, V> KeyValueView<K, V> keyValueView(Mapper<K> keyMapper, Mapper<V> valMapper);

    /**
     * Creates a key-value view of a table for the binary object concept.
     *
     * @return Table key-value view.
     */
    KeyValueView<Tuple, Tuple> keyValueView();

    /**
     * Creates a key-value view of a table for the key and value classes provided.
     *
     * @param keyCls Key class.
     * @param valCls Value class.
     * @param <K>    Key type.
     * @param <V>    Value type.
     * @return Table key-value view.
     */
    default <K, V> KeyValueView<K, V> keyValueView(Class<K> keyCls, Class<V> valCls) {
        return keyValueView(Mapper.of(keyCls), Mapper.of(valCls));
    }
}
