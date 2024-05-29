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

import org.apache.ignite.internal.marshaller.Marshaller;
import org.apache.ignite.internal.marshaller.MarshallerException;
import org.apache.ignite.internal.marshaller.MarshallerSchema;
import org.apache.ignite.internal.marshaller.MarshallersProvider;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.KvMarshaller;
import org.apache.ignite.internal.schema.row.Row;
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

    /** Positions of key fields in the schema. */
    private final int[] keyPositions;

    /** Positions of value fields in the schema. */
    private final int[] valPositions;

    /**
     * Creates KV marshaller.
     *
     * @param schema Schema descriptor.
     * @param marshallers Marshallers provider.
     * @param keyMapper Mapper for key objects.
     * @param valueMapper Mapper for value objects.
     */
    public KvMarshallerImpl(SchemaDescriptor schema, MarshallersProvider marshallers, Mapper<K> keyMapper, Mapper<V> valueMapper) {
        this.schema = schema;

        keyClass = keyMapper.targetType();
        valClass = valueMapper.targetType();

        MarshallerSchema marshallerSchema = schema.marshallerSchema();
        keyMarsh = marshallers.getKeysMarshaller(marshallerSchema, keyMapper, true, false);
        valMarsh = marshallers.getValuesMarshaller(marshallerSchema, valueMapper, true, false);
        keyPositions = schema.keyColumns().stream().mapToInt(Column::positionInRow).toArray();
        valPositions = schema.valueColumns().stream().mapToInt(Column::positionInRow).toArray();
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

        MarshallerRowBuilder rowBuilder = createRowBuilder(key);

        return Row.wrapKeyOnlyBinaryRow(schema, rowBuilder.build());
    }

    /** {@inheritDoc} */
    @Override
    public Row marshal(K key, @Nullable V val) throws MarshallerException {
        assert keyClass.isInstance(key);
        assert val == null || valClass.isInstance(val);

        MarshallerRowBuilder rowBuilder = createRowBuilder(key, val);

        return Row.wrapBinaryRow(schema, rowBuilder.build());
    }

    /** {@inheritDoc} */
    @Override
    public K unmarshalKeyOnly(Row row) throws MarshallerException {
        assert row.elementCount() == keyPositions.length : "Number of key columns does not match";

        Object o = keyMarsh.readObject(new RowReader(row), null);

        assert keyClass.isInstance(o);

        return (K) o;
    }

    /** {@inheritDoc} */
    @Override
    public K unmarshalKey(Row row) throws MarshallerException {
        Object o = keyMarsh.readObject(new RowReader(row, keyPositions), null);

        assert keyClass.isInstance(o);

        return (K) o;
    }

    /** {@inheritDoc} */
    @Nullable
    @Override
    public V unmarshalValue(Row row) throws MarshallerException {
        Object o = valMarsh.readObject(new RowReader(row, valPositions), null);

        assert o == null || valClass.isInstance(o);

        return (V) o;
    }

    /** {@inheritDoc} */
    @Override
    public @Nullable Object value(Object obj, int fldIdx) {
        Column column = schema.column(fldIdx);
        return column.positionInKey() >= 0
                ? keyMarsh.value(obj, column.positionInKey())
                : valMarsh.value(obj, column.positionInValue());
    }

    /**
     * Creates {@link MarshallerRowBuilder} for key.
     *
     * @param key Key object.
     * @return Row assembler.
     * @throws MarshallerException If failed to read key or value object content.
     */
    private MarshallerRowBuilder createRowBuilder(Object key) throws MarshallerException {
        try {
            return ObjectStatistics.createRowBuilder(schema, keyMarsh, key);
        } catch (Throwable e) {
            throw new MarshallerException(e.getMessage(), e);
        }
    }

    /**
     * Creates {@link MarshallerRowBuilder} for key-value pair.
     *
     * @param key Key object.
     * @param val Value object.
     * @return Row assembler.
     * @throws MarshallerException If failed to read key or value object content.
     */
    private MarshallerRowBuilder createRowBuilder(Object key, @Nullable Object val) throws MarshallerException {
        try {
            return ObjectStatistics.createRowBuilder(schema, keyMarsh, valMarsh, key, val);
        } catch (Throwable e) {
            throw new MarshallerException(e.getMessage(), e);
        }
    }
}
