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

import static org.apache.ignite.internal.schema.marshaller.MarshallerUtil.getValueSize;

import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.KvMarshaller;
import org.apache.ignite.internal.schema.marshaller.MarshallerException;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.table.mapper.Mapper;
import org.jetbrains.annotations.NotNull;
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
     * @param schema      Schema descriptor.
     * @param keyMapper   Mapper for key objects.
     * @param valueMapper Mapper for value objects.
     */
    public KvMarshallerImpl(SchemaDescriptor schema, @NotNull Mapper<K> keyMapper, @NotNull Mapper<V> valueMapper) {
        this.schema = schema;

        keyClass = keyMapper.targetType();
        valClass = valueMapper.targetType();

        keyMarsh = Marshaller.createMarshaller(schema.keyColumns().columns(), keyMapper, true);
        valMarsh = Marshaller.createMarshaller(schema.valueColumns().columns(), valueMapper, false);
    }

    /** {@inheritDoc} */
    @Override
    public int schemaVersion() {
        return schema.version();
    }

    /** {@inheritDoc} */
    @Override
    public Row marshal(@NotNull K key) throws MarshallerException {
        assert keyClass.isInstance(key);

        final RowAssembler asm = createAssembler(key, null);

        keyMarsh.writeObject(key, asm);

        return new Row(schema, new ByteBufferRow(asm.toBytes()));
    }

    /** {@inheritDoc} */
    @Override
    public Row marshal(@NotNull K key, V val) throws MarshallerException {
        assert keyClass.isInstance(key);
        assert val == null || valClass.isInstance(val);

        final RowAssembler asm = createAssembler(key, val);
        keyMarsh.writeObject(key, asm);
        valMarsh.writeObject(val, asm);
        return new Row(schema, new ByteBufferRow(asm.toBytes()));
    }

    /** {@inheritDoc} */
    @NotNull
    @Override
    public K unmarshalKey(@NotNull Row row) throws MarshallerException {
        final Object o = keyMarsh.readObject(row);

        assert keyClass.isInstance(o);

        return (K) o;
    }

    /** {@inheritDoc} */
    @Nullable
    @Override
    public V unmarshalValue(@NotNull Row row) throws MarshallerException {
        if (!row.hasValue()) {
            return null;
        }

        final Object o = valMarsh.readObject(row);

        assert o == null || valClass.isInstance(o);

        return (V) o;
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
        ObjectStatistic keyStat = collectObjectStats(schema.keyColumns(), keyMarsh, key);
        ObjectStatistic valStat = collectObjectStats(schema.valueColumns(), valMarsh, val);

        return new RowAssembler(schema, keyStat.nonNullColsSize, keyStat.nonNullCols,
                valStat.nonNullColsSize, valStat.nonNullCols);
    }

    /**
     * Reads object fields and gather statistic.
     *
     * @throws MarshallerException If failed to read object content.
     */
    private ObjectStatistic collectObjectStats(Columns cols, Marshaller marsh, Object obj) throws MarshallerException {
        if (obj == null || !cols.hasVarlengthColumns()) {
            return ObjectStatistic.ZERO_VARLEN_STATISTICS;
        }

        int cnt = 0;
        int size = 0;

        for (int i = cols.firstVarlengthColumn(); i < cols.length(); i++) {
            final Object val = marsh.value(obj, i);
            final NativeType colType = cols.column(i).type();

            if (val == null || colType.spec().fixedLength()) {
                continue;
            }

            size += getValueSize(val, colType);
            cnt++;
        }

        return new ObjectStatistic(cnt, size);
    }

    /**
     * Object statistic.
     */
    private static class ObjectStatistic {
        /** Cached zero statistics. */
        static final ObjectStatistic ZERO_VARLEN_STATISTICS = new ObjectStatistic(0, 0);

        /** Non-null columns of varlen type. */
        int nonNullCols;

        /** Length of all non-null columns of varlen types. */
        int nonNullColsSize;

        /** Constructor. */
        ObjectStatistic(int nonNullCols, int nonNullColsSize) {
            this.nonNullCols = nonNullCols;
            this.nonNullColsSize = nonNullColsSize;
        }
    }
}
