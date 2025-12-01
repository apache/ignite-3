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

import java.util.Objects;
import org.apache.ignite.internal.marshaller.Marshaller;
import org.apache.ignite.internal.marshaller.MarshallerSchema;
import org.apache.ignite.internal.marshaller.MarshallersProvider;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.marshaller.RecordMarshaller;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.lang.MarshallerException;
import org.apache.ignite.table.mapper.Mapper;
import org.apache.ignite.table.mapper.PojoMapper;
import org.jetbrains.annotations.Nullable;

/**
 * Record marshaller for given schema and mappers.
 *
 * @param <R> Record type.
 */
public class RecordMarshallerImpl<R> implements RecordMarshaller<R> {
    /** Schema. */
    private final SchemaDescriptor schema;

    /** Key marshaller. */
    private final Marshaller keyMarsh;

    /** Value marshaller. */
    private final Marshaller valMarsh;

    /** Record marshaller. */
    private final Marshaller recMarsh;

    /** Record type. */
    private final Class<R> recClass;

    /** Positions of key fields in the schema. */
    private final int[] keyPositions;

    /**
     * Creates KV marshaller.
     *
     * @param schema Schema descriptor.
     * @param marshallers Marshaller provider.
     * @param mapper Mapper for record objects.
     */
    public RecordMarshallerImpl(SchemaDescriptor schema, MarshallersProvider marshallers, Mapper<R> mapper) {
        assert mapper instanceof PojoMapper;

        this.schema = schema;

        recClass = mapper.targetType();

        MarshallerSchema marshallerSchema = schema.marshallerSchema();

        keyMarsh = marshallers.getKeysMarshaller(marshallerSchema, mapper, true, true);
        valMarsh = marshallers.getValuesMarshaller(marshallerSchema, mapper, false, true);
        recMarsh = marshallers.getRowMarshaller(marshallerSchema, mapper, false, false);
        keyPositions = schema.keyColumns().stream().mapToInt(Column::positionInKey).toArray();
    }

    /** {@inheritDoc} */
    @Override
    public int schemaVersion() {
        return schema.version();
    }

    /** {@inheritDoc} */
    @Override
    public Row marshal(R rec) throws MarshallerException {
        assert recClass.isInstance(rec);

        RowAssembler asm = createAssembler(Objects.requireNonNull(rec));

        recMarsh.writeObject(rec, new RowWriter(asm));

        return Row.wrapBinaryRow(schema, asm.build());
    }

    /** {@inheritDoc} */
    @Override
    public Row marshalKey(R rec) throws MarshallerException {
        assert recClass.isInstance(rec);

        RowAssembler asm = createAssemblerForKey(Objects.requireNonNull(rec));

        keyMarsh.writeObject(rec, new RowWriter(asm));

        return Row.wrapKeyOnlyBinaryRow(schema, asm.build());
    }

    /** {@inheritDoc} */
    @Override
    public R unmarshal(Row row) throws MarshallerException {
        Marshaller marsh = row.keyOnly() ? keyMarsh : recMarsh;
        RowReader reader = row.keyOnly() ? new RowReader(row, keyPositions) : new RowReader(row);
        final Object o = marsh.readObject(reader);

        assert recClass.isInstance(o);

        return (R) o;
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
     * Creates {@link RowAssembler} for key.
     *
     * @param key Key object.
     * @return Row assembler.
     * @throws MarshallerException If failed to read key object content.
     */
    private RowAssembler createAssemblerForKey(Object key) throws MarshallerException {
        try {
            return ObjectStatistics.createAssembler(schema, keyMarsh, key);
        } catch (Throwable e) {
            throw new MarshallerException(e.getMessage(), e);
        }
    }

    /**
     * Creates {@link RowAssembler} for key-value pair.
     *
     * @param rec Record object.
     * @return Row assembler.
     * @throws MarshallerException If failed to read key or value object content.
     */
    private RowAssembler createAssembler(Object rec) throws MarshallerException {
        try {
            return ObjectStatistics.createAssembler(schema, keyMarsh, valMarsh, rec, rec);
        } catch (Throwable e) {
            throw new MarshallerException(e.getMessage(), e);
        }
    }
}
