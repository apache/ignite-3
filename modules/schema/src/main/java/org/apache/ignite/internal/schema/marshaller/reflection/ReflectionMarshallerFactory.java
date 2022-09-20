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

package org.apache.ignite.internal.schema.marshaller.reflection;

import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.KvMarshaller;
import org.apache.ignite.internal.schema.marshaller.MarshallerFactory;
import org.apache.ignite.internal.schema.marshaller.RecordMarshaller;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.NotNull;

/**
 * Factory for reflection-based marshaller.
 */
public class ReflectionMarshallerFactory implements MarshallerFactory {
    /** {@inheritDoc} */
    @Override
    public <K, V> KvMarshaller<K, V> create(SchemaDescriptor schema, @NotNull Mapper<K> keyMapper, @NotNull Mapper<V> valueMapper) {
        return new KvMarshallerImpl<>(schema, keyMapper, valueMapper);
    }

    /** {@inheritDoc} */
    @Override
    public <R> RecordMarshaller<R> create(SchemaDescriptor schema, @NotNull Mapper<R> mapper) {
        return new RecordMarshallerImpl<>(schema, mapper);
    }
}
