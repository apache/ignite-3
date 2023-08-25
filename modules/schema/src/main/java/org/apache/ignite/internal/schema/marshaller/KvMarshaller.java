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

package org.apache.ignite.internal.schema.marshaller;

import org.apache.ignite.internal.schema.row.Row;
import org.jetbrains.annotations.Nullable;

/**
 * Key-value marshaller interface provides method to marshal/unmarshal key and value objects to/from
 * a row.
 *
 * @param <K> Key type.
 * @param <V> Value type.
 */
public interface KvMarshaller<K, V> {
    /**
     * Returns marshaller schema version.
     */
    int schemaVersion();

    /**
     * Marshal given key and value objects to a table row.
     *
     * @param key Key object to marshal.
     * @return Table row with columns from given key-value pair.
     * @throws MarshallerException If failed to marshal key and/or value.
     */
    Row marshal(K key) throws MarshallerException;

    /**
     * Marshal given key and value objects to a table row.
     *
     * @param key Key object to marshal.
     * @param val Value object to marshal.
     * @return Table row with columns from given key-value pair.
     * @throws MarshallerException If failed to marshal key and/or value.
     */
    Row marshal(K key, V val) throws MarshallerException;

    /**
     * Unmarshal given row to a key object.
     *
     * @param row Table row.
     * @return Key object.
     * @throws MarshallerException If failed to unmarshal row.
     */
    K unmarshalKey(Row row) throws MarshallerException;

    /**
     * Unmarshal given row to a value object.
     *
     * @param row Table row.
     * @return Value object.
     * @throws MarshallerException If failed to unmarshal row.
     */
    @Nullable V unmarshalValue(Row row) throws MarshallerException;

    /**
     * Reads object field value.
     *
     * @param obj    Object to read from.
     * @param fldIdx Field index.
     * @return Field value.
     */
    @Nullable Object value(Object obj, int fldIdx) throws MarshallerException;
}
