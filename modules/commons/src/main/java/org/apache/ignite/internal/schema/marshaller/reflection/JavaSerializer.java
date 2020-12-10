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

package org.apache.ignite.internal.schema.marshaller.reflection;

import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.schema.ByteBufferTuple;
import org.apache.ignite.internal.schema.Columns;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.Tuple;
import org.apache.ignite.internal.schema.TupleAssembler;
import org.apache.ignite.internal.schema.marshaller.BinaryMode;
import org.apache.ignite.internal.schema.marshaller.SerializationException;
import org.apache.ignite.internal.schema.marshaller.Serializer;

import static org.apache.ignite.internal.schema.marshaller.MarshallerUtil.getValueSize;

/**
 * Reflection based (de)serializer.
 */
public class JavaSerializer implements Serializer {

    /**
     * Reads value object from tuple.
     *
     * @param reader Reader.
     * @param colIdx Column index.
     * @param mode Binary read mode.
     * @return Read value object.
     */
    static Object readRefValue(Tuple reader, int colIdx, BinaryMode mode) {
        assert reader != null;
        assert colIdx >= 0;

        Object val = null;

        switch (mode) {
            case BYTE:
                val = reader.byteValueBoxed(colIdx);

                break;

            case SHORT:
                val = reader.shortValueBoxed(colIdx);

                break;

            case INT:
                val = reader.intValueBoxed(colIdx);

                break;

            case LONG:
                val = reader.longValueBoxed(colIdx);

                break;

            case FLOAT:
                val = reader.floatValueBoxed(colIdx);

                break;

            case DOUBLE:
                val = reader.doubleValueBoxed(colIdx);

                break;

            case STRING:
                val = reader.stringValue(colIdx);

                break;

            case UUID:
                val = reader.uuidValue(colIdx);

                break;

            case BYTE_ARR:
                val = reader.bytesValue(colIdx);

                break;

            case BITSET:
                val = reader.bitmaskValue(colIdx);

                break;

            default:
                assert false : "Invalid mode: " + mode;
        }

        return val;
    }

    /**
     * Writes reference value to tuple.
     *
     * @param val Value object.
     * @param writer Writer.
     * @param mode Write binary mode.
     */
    static void writeRefObject(Object val, TupleAssembler writer, BinaryMode mode) {
        assert writer != null;

        if (val == null) {
            writer.appendNull();

            return;
        }

        switch (mode) {
            case BYTE:
                writer.appendByte((Byte)val);

                break;

            case SHORT:
                writer.appendShort((Short)val);

                break;

            case INT:
                writer.appendInt((Integer)val);

                break;

            case LONG:
                writer.appendLong((Long)val);

                break;

            case FLOAT:
                writer.appendFloat((Float)val);

                break;

            case DOUBLE:
                writer.appendDouble((Double)val);

                break;

            case STRING:
                writer.appendString((String)val);

                break;

            case UUID:
                writer.appendUuid((UUID)val);

                break;

            case BYTE_ARR:
                writer.appendBytes((byte[])val);

                break;

            case BITSET:
                writer.appendBitmask((BitSet)val);

                break;

            default:
                assert false : "Invalid mode: " + mode;
        }
    }

    /** Schema. */
    private final SchemaDescriptor schema;

    /** Key class. */
    private final Class<?> keyClass;

    /** Value class. */
    private final Class<?> valClass;

    /** Key marshaller. */
    private final Marshaller keyMarsh;

    /** Value marshaller. */
    private final Marshaller valMarsh;

    /**
     * Constructor.
     *
     * @param schema Schema.
     * @param keyClass Key type.
     * @param valClass Value type.
     */
    public JavaSerializer(SchemaDescriptor schema, Class<?> keyClass, Class<?> valClass) {
        this.schema = schema;
        this.keyClass = keyClass;
        this.valClass = valClass;

        keyMarsh = Marshaller.createMarshaller(schema.keyColumns(), 0, keyClass);
        valMarsh = Marshaller.createMarshaller(schema.valueColumns(), schema.keyColumns().length(), valClass);
    }

    /** {@inheritDoc} */
    @Override public byte[] serialize(Object key, Object val) throws SerializationException {
        assert keyClass.isInstance(key);
        assert val == null || valClass.isInstance(val);

        final TupleAssembler asm = createAssembler(key, val);

        keyMarsh.writeObject(key, asm);

        if (val != null)
            valMarsh.writeObject(val, asm);
        else
            assert false; // TODO: add tomstone support and remove assertion.

        return asm.build();
    }

    /**
     * Creates TupleAssebler for key-value pair.
     *
     * @param key Key object.
     * @param val Value object.
     * @return Tuple assembler.
     */
    private TupleAssembler createAssembler(Object key, Object val) {
        ObjectStatistic keyStat = collectObjectStats(schema.keyColumns(), keyMarsh, key);
        ObjectStatistic valStat = collectObjectStats(schema.valueColumns(), valMarsh, val);

        int size = TupleAssembler.tupleSize(
            schema.keyColumns(), keyStat.nonNullFields, keyStat.nonNullFieldsSize,
            schema.valueColumns(), valStat.nonNullFields, valStat.nonNullFieldsSize);

        return new TupleAssembler(schema, size, keyStat.nonNullFields, valStat.nonNullFields);
    }

    /**
     * Reads object fields and gather statistic.
     *
     * @param cols Schema columns.
     * @param marsh Marshaller.
     * @param obj Object.
     * @return Object statistic.
     */
    private ObjectStatistic collectObjectStats(Columns cols, Marshaller marsh, Object obj) {
        if (obj == null || cols.firstVarlengthColumn() < 0 /* No varlen columns */)
            return new ObjectStatistic(0, 0);

        int cnt = 0;
        int size = 0;

        for (int i = cols.firstVarlengthColumn(); i < cols.length(); i++) {
            final Object val = marsh.value(obj, i);

            if (val == null || cols.column(i).type().spec().fixedLength())
                continue;

            size += getValueSize(val, cols.column(i).type());
            cnt++;
        }

        return new ObjectStatistic(cnt, size);
    }

    /** {@inheritDoc} */
    @Override public Object deserializeKey(byte[] data) throws SerializationException {
        final Tuple tuple = new ByteBufferTuple(schema, data);

        final Object o = keyMarsh.readObject(tuple);

        assert keyClass.isInstance(o);

        return o;
    }

    /** {@inheritDoc} */
    @Override public Object deserializeValue(byte[] data) throws SerializationException {
        final Tuple tuple = new ByteBufferTuple(schema, data);

        // TODO: add tomstone support.

        final Object o = valMarsh.readObject(tuple);

        assert valClass.isInstance(o);

        return o;
    }

    /**
     * Object statistic.
     */
    private static class ObjectStatistic {
        /** Non-null fields of varlen type. */
        int nonNullFields;

        /** Length of all non-null fields of varlen types. */
        int nonNullFieldsSize;

        /** Constructor. */
        public ObjectStatistic(int nonNullFields, int nonNullFieldsSize) {
            this.nonNullFields = nonNullFields;
            this.nonNullFieldsSize = nonNullFieldsSize;
        }
    }
}
