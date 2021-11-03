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

package org.apache.ignite.internal.schema.marshaller;

import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.table.mapper.Mapper;

/**
 * Marshaller factory interface.
 */
@FunctionalInterface
public interface MarshallerFactory {
    /**
     * Creates key-value marshaller using provided mappers.
     *
     * @param schema Schema descriptor.
     * @param keyMapper Key mapper.
     * @param valueMapper Value mapper.
     * @return Key-value marshaller.
     */
    <K, V> KVMarshaller<K, V> create(SchemaDescriptor schema, Mapper<K> keyMapper, Mapper<V> valueMapper);

    /**
     * Shortcut method creates key-value marshaller for classes using identity mappers.
     *
     * @param schema Schema descriptor.
     * @param kClass Key type.
     * @param vClass Value type.
     * @return Key-value marshaller.
     * @see Mapper#identityMapper(Class)
     */
    default <K, V> KVMarshaller<K, V> create(SchemaDescriptor schema, Class<K> kClass, Class<V> vClass) {
        return create(schema, Mapper.identityMapper(kClass), Mapper.identityMapper(vClass));
    }
}
