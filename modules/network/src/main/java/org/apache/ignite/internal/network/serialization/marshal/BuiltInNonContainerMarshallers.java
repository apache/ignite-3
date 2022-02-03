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

package org.apache.ignite.internal.network.serialization.marshal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.BitSet;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.internal.network.serialization.ClassDescriptor;
import org.apache.ignite.internal.network.serialization.Null;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataOutput;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Encapsulates (un)marshalling logic for built-in types.
 */
class BuiltInNonContainerMarshallers {
    private final Map<Class<?>, BuiltInMarshaller<?>> builtInMarshallers = createBuiltInMarshallers();

    private static Map<Class<?>, BuiltInMarshaller<?>> createBuiltInMarshallers() {
        Map<Class<?>, BuiltInMarshaller<?>> map = new HashMap<>();

        addPrimitiveAndWrapper(map, byte.class, Byte.class, (obj, dos) -> dos.writeByte(obj), DataInput::readByte);
        addPrimitiveAndWrapper(map, short.class, Short.class, (obj, dos) -> dos.writeShort(obj), DataInput::readShort);
        addPrimitiveAndWrapper(map, int.class, Integer.class, (obj, dos) -> dos.writeInt(obj), DataInput::readInt);
        addPrimitiveAndWrapper(map, float.class, Float.class, (obj, dos) -> dos.writeFloat(obj), DataInput::readFloat);
        addPrimitiveAndWrapper(map, long.class, Long.class, (obj, dos) -> dos.writeLong(obj), DataInput::readLong);
        addPrimitiveAndWrapper(map, double.class, Double.class, (obj, dos) -> dos.writeDouble(obj), DataInput::readDouble);
        addPrimitiveAndWrapper(map, boolean.class, Boolean.class, (obj, dos) -> dos.writeBoolean(obj), DataInput::readBoolean);
        addPrimitiveAndWrapper(map, char.class, Character.class, (obj, dos) -> dos.writeChar(obj), DataInput::readChar);
        addSingle(map, Object.class, (obj, dos) -> {}, BuiltInMarshalling::readBareObject);
        addSingle(map, String.class, BuiltInMarshalling::writeString, BuiltInMarshalling::readString);
        addSingle(map, UUID.class, BuiltInMarshalling::writeUuid, BuiltInMarshalling::readUuid);
        addSingle(map, IgniteUuid.class, BuiltInMarshalling::writeIgniteUuid, BuiltInMarshalling::readIgniteUuid);
        addSingle(map, Date.class, BuiltInMarshalling::writeDate, BuiltInMarshalling::readDate);
        addSingle(map, byte[].class, BuiltInMarshalling::writeByteArray, BuiltInMarshalling::readByteArray);
        addSingle(map, short[].class, BuiltInMarshalling::writeShortArray, BuiltInMarshalling::readShortArray);
        addSingle(map, int[].class, BuiltInMarshalling::writeIntArray, BuiltInMarshalling::readIntArray);
        addSingle(map, float[].class, BuiltInMarshalling::writeFloatArray, BuiltInMarshalling::readFloatArray);
        addSingle(map, long[].class, BuiltInMarshalling::writeLongArray, BuiltInMarshalling::readLongArray);
        addSingle(map, double[].class, BuiltInMarshalling::writeDoubleArray, BuiltInMarshalling::readDoubleArray);
        addSingle(map, boolean[].class, BuiltInMarshalling::writeBooleanArray, BuiltInMarshalling::readBooleanArray);
        addSingle(map, char[].class, BuiltInMarshalling::writeCharArray, BuiltInMarshalling::readCharArray);
        addSingle(map, BigDecimal.class, BuiltInMarshalling::writeBigDecimal, BuiltInMarshalling::readBigDecimal);
        addSingle(map, BitSet.class, BuiltInMarshalling::writeBitSet, BuiltInMarshalling::readBitSet);
        addSingle(map, Null.class, (obj, output) -> {}, input -> null);
        addSingle(map, Class.class, (obj, out, ctx) -> BuiltInMarshalling.writeClass(obj, out), BuiltInMarshalling::readClass);

        return Map.copyOf(map);
    }

    private static <T> void addSingle(
            Map<Class<?>, BuiltInMarshaller<?>> map,
            Class<T> objectClass,
            ValueWriter<T> writer,
            ValueReader<T> reader
    ) {
        BuiltInMarshaller<T> builtInMarshaller = builtInMarshaller(objectClass, writer, reader);

        map.put(objectClass, builtInMarshaller);
    }

    private static <T> void addSingle(
            Map<Class<?>, BuiltInMarshaller<?>> map,
            Class<T> objectClass,
            ContextlessValueWriter<T> writer,
            ContextlessValueReader<T> reader
    ) {
        addSingle(map, objectClass, contextless(writer), contextless(reader));
    }

    private static <T> void addPrimitiveAndWrapper(
            Map<Class<?>, BuiltInMarshaller<?>> map,
            Class<?> primitiveClass,
            Class<T> wrapperClass,
            ContextlessValueWriter<T> writer,
            ContextlessValueReader<T> reader
    ) {
        BuiltInMarshaller<T> builtInMarshaller = builtInMarshaller(wrapperClass, contextless(writer), contextless(reader));

        map.put(primitiveClass, builtInMarshaller);
        map.put(wrapperClass, builtInMarshaller);
    }

    private static <T> ValueWriter<T> contextless(ContextlessValueWriter<T> writer) {
        return (obj, out, ctx) -> writer.write(obj, out);
    }

    private static <T> ValueReader<T> contextless(ContextlessValueReader<T> reader) {
        return (in, ctx) -> reader.read(in);
    }

    private static <T> BuiltInMarshaller<T> builtInMarshaller(Class<T> valueRefClass, ValueWriter<T> writer, ValueReader<T> reader) {
        return new BuiltInMarshaller<>(valueRefClass, writer, reader);
    }

    /**
     * Returns {@code true} if the given descriptor is a built-in we can handle.
     *
     * @param descriptor class descriptor to check
     * @return {@code true} if we the given descriptor is a built-in we can handle
     */
    boolean supports(ClassDescriptor descriptor) {
        return descriptor.isEnum() || descriptor.isLatin1String()
                || builtInMarshallers.containsKey(descriptor.clazz());
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
        if (descriptor.isEnum()) {
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
        if (descriptor.isEnum()) {
            return readEnum(descriptor, input);
        }

        BuiltInMarshaller<?> builtinMarshaller = findBuiltInMarshaller(descriptor);
        return builtinMarshaller.unmarshal(input, context);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Object readEnum(ClassDescriptor descriptor, DataInput input) throws IOException {
        return BuiltInMarshalling.readEnum(input, (Class<? extends Enum>) descriptor.clazz());
    }

    private BuiltInMarshaller<?> findBuiltInMarshaller(ClassDescriptor descriptor) {
        BuiltInMarshaller<?> builtinMarshaller = builtInMarshallers.get(descriptor.clazz());
        if (builtinMarshaller == null) {
            throw new IllegalStateException("No support for (un)marshalling " + descriptor.clazz() + ", but it's marked as built-in");
        }
        return builtinMarshaller;
    }

    private static class BuiltInMarshaller<T> {
        private final Class<T> valueRefClass;
        private final ValueWriter<T> writer;
        private final ValueReader<T> reader;

        private BuiltInMarshaller(Class<T> valueRefClass, ValueWriter<T> writer, ValueReader<T> reader) {
            this.valueRefClass = valueRefClass;
            this.writer = writer;
            this.reader = reader;
        }

        private void marshal(Object object, IgniteDataOutput output, MarshallingContext context) throws IOException, MarshalException {
            writer.write(valueRefClass.cast(object), output, context);
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
