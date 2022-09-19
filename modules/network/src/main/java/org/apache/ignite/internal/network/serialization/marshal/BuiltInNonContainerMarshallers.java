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

package org.apache.ignite.internal.network.serialization.marshal;

import static org.apache.ignite.internal.network.serialization.BuiltInType.BARE_OBJECT;
import static org.apache.ignite.internal.network.serialization.BuiltInType.BIT_SET;
import static org.apache.ignite.internal.network.serialization.BuiltInType.BOOLEAN;
import static org.apache.ignite.internal.network.serialization.BuiltInType.BOOLEAN_ARRAY;
import static org.apache.ignite.internal.network.serialization.BuiltInType.BOOLEAN_BOXED;
import static org.apache.ignite.internal.network.serialization.BuiltInType.BYTE;
import static org.apache.ignite.internal.network.serialization.BuiltInType.BYTE_ARRAY;
import static org.apache.ignite.internal.network.serialization.BuiltInType.BYTE_BOXED;
import static org.apache.ignite.internal.network.serialization.BuiltInType.CHAR;
import static org.apache.ignite.internal.network.serialization.BuiltInType.CHAR_ARRAY;
import static org.apache.ignite.internal.network.serialization.BuiltInType.CHAR_BOXED;
import static org.apache.ignite.internal.network.serialization.BuiltInType.CLASS;
import static org.apache.ignite.internal.network.serialization.BuiltInType.DATE;
import static org.apache.ignite.internal.network.serialization.BuiltInType.DECIMAL;
import static org.apache.ignite.internal.network.serialization.BuiltInType.DOUBLE;
import static org.apache.ignite.internal.network.serialization.BuiltInType.DOUBLE_ARRAY;
import static org.apache.ignite.internal.network.serialization.BuiltInType.DOUBLE_BOXED;
import static org.apache.ignite.internal.network.serialization.BuiltInType.FLOAT;
import static org.apache.ignite.internal.network.serialization.BuiltInType.FLOAT_ARRAY;
import static org.apache.ignite.internal.network.serialization.BuiltInType.FLOAT_BOXED;
import static org.apache.ignite.internal.network.serialization.BuiltInType.IGNITE_UUID;
import static org.apache.ignite.internal.network.serialization.BuiltInType.INT;
import static org.apache.ignite.internal.network.serialization.BuiltInType.INT_ARRAY;
import static org.apache.ignite.internal.network.serialization.BuiltInType.INT_BOXED;
import static org.apache.ignite.internal.network.serialization.BuiltInType.LONG;
import static org.apache.ignite.internal.network.serialization.BuiltInType.LONG_ARRAY;
import static org.apache.ignite.internal.network.serialization.BuiltInType.LONG_BOXED;
import static org.apache.ignite.internal.network.serialization.BuiltInType.NULL;
import static org.apache.ignite.internal.network.serialization.BuiltInType.SHORT;
import static org.apache.ignite.internal.network.serialization.BuiltInType.SHORT_ARRAY;
import static org.apache.ignite.internal.network.serialization.BuiltInType.SHORT_BOXED;
import static org.apache.ignite.internal.network.serialization.BuiltInType.STRING;
import static org.apache.ignite.internal.network.serialization.BuiltInType.UUID;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.network.serialization.BuiltInType;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Encapsulates (un)marshalling logic for built-in types.
 */
class BuiltInNonContainerMarshallers {
    private final Int2ObjectMap<BuiltInMarshaller<?>> builtInMarshallers = createBuiltInMarshallers();

    private static Int2ObjectMap<BuiltInMarshaller<?>> createBuiltInMarshallers() {
        Map<Integer, BuiltInMarshaller<?>> map = new HashMap<>();

        addPrimitiveAndWrapper(map, BYTE, BYTE_BOXED, (obj, dos) -> dos.writeByte(obj), DataInput::readByte);
        addPrimitiveAndWrapper(map, SHORT, SHORT_BOXED, (obj, dos) -> dos.writeShort(obj), DataInput::readShort);
        addPrimitiveAndWrapper(map, INT, INT_BOXED, (obj, dos) -> dos.writeInt(obj), DataInput::readInt);
        addPrimitiveAndWrapper(map, FLOAT, FLOAT_BOXED, (obj, dos) -> dos.writeFloat(obj), DataInput::readFloat);
        addPrimitiveAndWrapper(map, LONG, LONG_BOXED, (obj, dos) -> dos.writeLong(obj), DataInput::readLong);
        addPrimitiveAndWrapper(map, DOUBLE, DOUBLE_BOXED, (obj, dos) -> dos.writeDouble(obj), DataInput::readDouble);
        addPrimitiveAndWrapper(map, BOOLEAN, BOOLEAN_BOXED, (obj, dos) -> dos.writeBoolean(obj), DataInput::readBoolean);
        addPrimitiveAndWrapper(map, CHAR, CHAR_BOXED, (obj, dos) -> dos.writeChar(obj), DataInput::readChar);
        addSingle(map, BARE_OBJECT, (obj, dos) -> {}, BuiltInMarshalling::readBareObject);
        addSingle(map, STRING, BuiltInMarshalling::writeString, BuiltInMarshalling::readString);
        addSingle(map, UUID, BuiltInMarshalling::writeUuid, BuiltInMarshalling::readUuid);
        addSingle(map, IGNITE_UUID, BuiltInMarshalling::writeIgniteUuid, BuiltInMarshalling::readIgniteUuid);
        addSingle(map, DATE, BuiltInMarshalling::writeDate, BuiltInMarshalling::readDate);
        addSingle(map, BYTE_ARRAY, BuiltInMarshalling::writeByteArray, BuiltInMarshalling::readByteArray);
        addSingle(map, SHORT_ARRAY, BuiltInMarshalling::writeShortArray, BuiltInMarshalling::readShortArray);
        addSingle(map, INT_ARRAY, BuiltInMarshalling::writeIntArray, BuiltInMarshalling::readIntArray);
        addSingle(map, FLOAT_ARRAY, BuiltInMarshalling::writeFloatArray, BuiltInMarshalling::readFloatArray);
        addSingle(map, LONG_ARRAY, BuiltInMarshalling::writeLongArray, BuiltInMarshalling::readLongArray);
        addSingle(map, DOUBLE_ARRAY, BuiltInMarshalling::writeDoubleArray, BuiltInMarshalling::readDoubleArray);
        addSingle(map, BOOLEAN_ARRAY, BuiltInMarshalling::writeBooleanArray, BuiltInMarshalling::readBooleanArray);
        addSingle(map, CHAR_ARRAY, BuiltInMarshalling::writeCharArray, BuiltInMarshalling::readCharArray);
        addSingle(map, DECIMAL, BuiltInMarshalling::writeBigDecimal, BuiltInMarshalling::readBigDecimal);
        addSingle(map, BIT_SET, BuiltInMarshalling::writeBitSet, BuiltInMarshalling::readBitSet);
        addSingle(map, NULL, (obj, output) -> {}, input -> null);
        addSingle(map, CLASS, (obj, out, ctx) -> BuiltInMarshalling.writeClass(obj, out), BuiltInMarshalling::readClass);

        return Int2ObjectMaps.unmodifiable(new Int2ObjectOpenHashMap<>(map));
    }

    private static <T> void addSingle(
            Map<Integer, BuiltInMarshaller<?>> map,
            BuiltInType type,
            ValueWriter<T> writer,
            ValueReader<T> reader
    ) {
        BuiltInMarshaller<T> builtInMarshaller = builtInMarshaller(writer, reader);

        map.put(type.descriptorId(), builtInMarshaller);
    }

    private static <T> void addSingle(
            Map<Integer, BuiltInMarshaller<?>> map,
            BuiltInType type,
            ContextlessValueWriter<T> writer,
            ContextlessValueReader<T> reader
    ) {
        addSingle(map, type, contextless(writer), contextless(reader));
    }

    private static <T> void addPrimitiveAndWrapper(
            Map<Integer, BuiltInMarshaller<?>> map,
            BuiltInType primitiveType,
            BuiltInType wrapperType,
            ContextlessValueWriter<T> writer,
            ContextlessValueReader<T> reader
    ) {
        BuiltInMarshaller<T> builtInMarshaller = builtInMarshaller(contextless(writer), contextless(reader));

        map.put(primitiveType.descriptorId(), builtInMarshaller);
        map.put(wrapperType.descriptorId(), builtInMarshaller);
    }

    private static <T> ValueWriter<T> contextless(ContextlessValueWriter<T> writer) {
        return (obj, out, ctx) -> writer.write(obj, out);
    }

    private static <T> ValueReader<T> contextless(ContextlessValueReader<T> reader) {
        return (in, ctx) -> reader.read(in);
    }

    private static <T> BuiltInMarshaller<T> builtInMarshaller(ValueWriter<T> writer, ValueReader<T> reader) {
        return new BuiltInMarshaller<>(writer, reader);
    }

    /**
     * Returns {@code true} if the given descriptor is a built-in we can handle.
     *
     * @param descriptor class descriptor to check
     * @return {@code true} if we the given descriptor is a built-in we can handle
     */
    boolean supports(ClassDescriptor descriptor) {
        return descriptor.isRuntimeEnum() || descriptor.isLatin1String()
                || builtInMarshallers.containsKey(descriptor.descriptorId());
    }

    void writeBuiltIn(Object object, ClassDescriptor descriptor, IgniteDataOutput output, MarshallingContext context)
            throws IOException, MarshalException {
        actuallyWrite(object, descriptor, output, context);

        context.addUsedDescriptor(descriptor);
    }

    private void actuallyWrite(Object object, ClassDescriptor descriptor, IgniteDataOutput output, MarshallingContext context)
            throws IOException, MarshalException {
        if (descriptor.isLatin1String()) {
            BuiltInMarshalling.writeLatin1String((String) object, output);
            return;
        }
        if (descriptor.isRuntimeEnum()) {
            BuiltInMarshalling.writeEnum((Enum<?>) object, output);
            return;
        }

        writeWithBuiltInMarshaller(object, descriptor, output, context);
    }

    private void writeWithBuiltInMarshaller(Object object, ClassDescriptor descriptor, IgniteDataOutput output, MarshallingContext context)
            throws IOException, MarshalException {
        BuiltInMarshaller<?> builtInMarshaller = findBuiltInMarshaller(descriptor);

        builtInMarshaller.marshal(object, output, context);
    }

    Object readBuiltIn(ClassDescriptor descriptor, IgniteDataInput input, UnmarshallingContext context)
            throws IOException, UnmarshalException {
        if (descriptor.isLatin1String()) {
            return BuiltInMarshalling.readLatin1String(input);
        }
        if (descriptor.isRuntimeEnum()) {
            return readEnum(descriptor, input);
        }

        BuiltInMarshaller<?> builtinMarshaller = findBuiltInMarshaller(descriptor);
        return builtinMarshaller.unmarshal(input, context);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Object readEnum(ClassDescriptor descriptor, DataInput input) throws IOException {
        return BuiltInMarshalling.readEnum(input, (Class<? extends Enum>) descriptor.localClass());
    }

    private BuiltInMarshaller<?> findBuiltInMarshaller(ClassDescriptor descriptor) {
        BuiltInMarshaller<?> builtinMarshaller = builtInMarshallers.get(descriptor.descriptorId());
        if (builtinMarshaller == null) {
            throw new IllegalStateException("No support for (un)marshalling " + descriptor.className() + ", but it's marked as built-in");
        }
        return builtinMarshaller;
    }

    private static class BuiltInMarshaller<T> {
        private final ValueWriter<T> writer;
        private final ValueReader<T> reader;

        private BuiltInMarshaller(ValueWriter<T> writer, ValueReader<T> reader) {
            this.writer = writer;
            this.reader = reader;
        }

        @SuppressWarnings("unchecked")
        private void marshal(Object object, IgniteDataOutput output, MarshallingContext context) throws IOException, MarshalException {
            writer.write((T) object, output, context);
        }

        private Object unmarshal(IgniteDataInput input, UnmarshallingContext context) throws IOException, UnmarshalException {
            return reader.read(input, context);
        }
    }

    interface ContextlessValueWriter<T> {
        /**
         * Writes the given value to a {@link DataOutput}.
         *
         * @param value     value to write
         * @param output    where to write to
         * @throws IOException      if an I/O problem occurs
         * @throws MarshalException if another problem occurs
         */
        void write(T value, IgniteDataOutput output) throws IOException, MarshalException;
    }


    private interface ContextlessValueReader<T> {
        /**
         * Reads the next value from a {@link DataInput}.
         *
         * @param input     from where to read
         * @return the value that was read
         * @throws IOException          if an I/O problem occurs
         * @throws UnmarshalException   if another problem (like {@link ClassNotFoundException}) occurs
         */
        T read(IgniteDataInput input) throws IOException, UnmarshalException;
    }
}
