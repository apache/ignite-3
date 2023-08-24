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
import org.apache.ignite.internal.schema.marshaller.MarshallerException;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.Nullable;

/**
 * Key-value marshaller for given schema and mappers.
 *
 * @param <K> Key type.
 * @param <V> Value type.
 */
public class KvMarshallerImpl<K, V> implements KvMarshaller<K, V> {
    /** Schema. */
    private final SchemaDescriptor schema;

    /** Key marshaller. */
    private final Marshaller keyMarsh;

    /** Value marshaller. */
    private final Marshaller valMarsh;

    /** Key type. */
    private final Class<K> keyClass;

    /** Value type. */
    private final Class<V> valClass;

    /**
     * Creates KV marshaller.
     *
     * @param schema Schema descriptor.
     * @param keyMapper Mapper for key objects.
     * @param valueMapper Mapper for value objects.
     */
    public KvMarshallerImpl(SchemaDescriptor schema, Mapper<K> keyMapper, Mapper<V> valueMapper) {
        this.schema = schema;

        keyClass = keyMapper.targetType();
        valClass = valueMapper.targetType();

        keyMarsh = Marshaller.createMarshaller(schema.keyColumns().columns(), keyMapper, true, false);
        valMarsh = Marshaller.createMarshaller(schema.valueColumns().columns(), valueMapper, true, false);
    }

    /** {@inheritDoc} */
    @Override
    public int schemaVersion() {
        return schema.version();
    }

    /** {@inheritDoc} */
    @Override
    public Row marshal(K key) throws MarshallerException {
        assert keyClass.isInstance(key);

        RowAssembler asm = createAssembler(key);

        keyMarsh.writeObject(key, asm);

        return Row.wrapKeyOnlyBinaryRow(schema, asm.build());
    }

    /** {@inheritDoc} */
    @Override
    public Row marshal(K key, V val) throws MarshallerException {
        assert keyClass.isInstance(key);
        assert val == null || valClass.isInstance(val);

        RowAssembler asm = createAssembler(key, val);
        keyMarsh.writeObject(key, asm);
        valMarsh.writeObject(val, asm);

        return Row.wrapBinaryRow(schema, asm.build());
    }

    /** {@inheritDoc} */
    @Override
    public K unmarshalKey(Row row) throws MarshallerException {
        Object o = keyMarsh.readObject(row);

        assert keyClass.isInstance(o);

        return (K) o;
    }

    /** {@inheritDoc} */
    @Nullable
    @Override
    public V unmarshalValue(Row row) throws MarshallerException {
        Object o = valMarsh.readObject(row);

        assert o == null || valClass.isInstance(o);

        return (V) o;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Object value(Object obj, int fldIdx) throws MarshallerException {
        return schema.isKeyColumn(fldIdx)
                ? keyMarsh.value(obj, fldIdx)
                : valMarsh.value(obj, fldIdx - schema.keyColumns().length());
    }

    /**
     * Creates {@link RowAssembler} for key.
     *
     * @param key Key object.
     * @return Row assembler.
     * @throws MarshallerException If failed to read key or value object content.
     */
    private RowAssembler createAssembler(Object key) throws MarshallerException {
        try {
            return ObjectStatistics.createAssembler(schema, keyMarsh, key);
        } catch (Throwable e) {
            throw new MarshallerException(e.getMessage(), e);
        }
    }

    /**
     * Creates {@link RowAssembler} for key-value pair.
     *
     * @param key Key object.
     * @param val Value object.
     * @return Row assembler.
     * @throws MarshallerException If failed to read key or value object content.
     */
    private RowAssembler createAssembler(Object key, Object val) throws MarshallerException {
        try {
            return ObjectStatistics.createAssembler(schema, keyMarsh, valMarsh, key, val);
        } catch (Throwable e) {
            throw new MarshallerException(e.getMessage(), e);
        }
    }
}
