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
import org.apache.ignite.table.partition.PartitionDistribution;
import org.apache.ignite.table.partition.PartitionManager;

/**
 * Table provides different views (key-value vs record) and approaches (mapped-object vs binary) to access the data.
 *
 * <p>Binary table views might be useful in the various scenarios, including when the user key-value classes
 * are not in classpath and/or when deserialization of the entire table record is undesirable for performance reasons.
 *
 * <p>Key-value views project table rows into key-value pairs, where all key columns are mapped to the key object,
 * and the rest of the columns are mapped to the value object.
 * Record and key-value views provide identical functionality, the projection is performed on the client side.
 *
 * @see RecordView
 * @see KeyValueView
 * @see org.apache.ignite.table.mapper.Mapper
 */
public interface Table {
    /**
     * Gets the canonical name of the table ([schema_name].[table_name]) with SQL-parser style quotation.
     *
     * <p>E.g. "PUBLIC.TBL0" - for TBL0 table in PUBLIC schema (both names are case insensitive),
     * "\"MySchema\".\"Tbl0\"" - for Tbl0 table in MySchema schema (both names are case sensitive), etc.
     *
     * @return Canonical table name.
     */
    default String name() {
        return qualifiedName().toCanonicalForm();
    }

    /**
     * Gets the qualified name of the table.
     *
     * @return Qualified name of the table.
     */
    QualifiedName qualifiedName();

    /**
     * Gets the partition manager.
     *
     * @return Partition manager.
     */
    @Deprecated
    PartitionManager partitionManager();

    /**
     * Gets the partition distribution.
     *
     * @return Partition distribution.
     */
    PartitionDistribution partitionDistribution();

    /**
     * Gets a record view of the table using the specified record class mapper.
     *
     * @param recMapper Record class mapper.
     * @param <R> Record type.
     * @return Table record view.
     */
    <R> RecordView<R> recordView(Mapper<R> recMapper);

    /**
     * Gets a record view of the table.
     *
     * @return Table record view.
     */
    RecordView<Tuple> recordView();

    /**
     * Gets a record view of the table using the default mapper for the specified record class.
     *
     * @param recCls Record class.
     * @param <R> Record type.
     * @return Table record view.
     */
    default <R> RecordView<R> recordView(Class<R> recCls) {
        return recordView(Mapper.of(recCls));
    }

    /**
     * Gets a key-value view of the table using the specified key-value class mappers.
     *
     * @param keyMapper Key class mapper.
     * @param valMapper Value class mapper.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Table key-value view.
     */
    <K, V> KeyValueView<K, V> keyValueView(Mapper<K> keyMapper, Mapper<V> valMapper);

    /**
     * Gets a key-value view of the table.
     *
     * @return Table key-value view.
     */
    KeyValueView<Tuple, Tuple> keyValueView();

    /**
     * Gets a key-value view of the table using the default mapper for the specified key and value classes.
     *
     * @param keyCls Key class.
     * @param valCls Value class.
     * @param <K> Key type.
     * @param <V> Value type.
     * @return Table key-value view.
     */
    default <K, V> KeyValueView<K, V> keyValueView(Class<K> keyCls, Class<V> valCls) {
        return keyValueView(Mapper.of(keyCls), Mapper.of(valCls));
    }
}
