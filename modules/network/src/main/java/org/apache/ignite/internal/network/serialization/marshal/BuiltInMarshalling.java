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

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.util.Collections.singletonList;
import static org.apache.ignite.internal.network.serialization.marshal.ProtocolMarshalling.readLength;
import static org.apache.ignite.internal.network.serialization.marshal.ProtocolMarshalling.writeLength;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.BitSet;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;
import java.util.UUID;
import java.util.function.IntFunction;
import org.apache.ignite.internal.lang.IgniteUuid;
import org.apache.ignite.internal.util.StringIntrospection;
import org.apache.ignite.internal.util.io.IgniteDataInput;
import org.apache.ignite.internal.util.io.IgniteDataInput.Materializer;
import org.apache.ignite.internal.util.io.IgniteDataOutput;

/**
 * Built-in types marshalling.
 */
class BuiltInMarshalling {
    private static final ValueWriter<Class<?>> classWriter = (obj, out, ctx) -> writeClass(obj, out);
    private static final IntFunction<Class<?>[]> classArrayFactory = Class[]::new;
    private static final ValueReader<Class<?>> classReader = BuiltInMarshalling::readClass;

    private static final Materializer<String> LATIN1_MATERIALIZER = (bytes, offset, len) -> new String(bytes, offset, len, ISO_8859_1);

    private static final Field singletonListElementField;

    static {
        try {
            singletonListElementField = singletonList(null).getClass().getDeclaredField("element");
            singletonListElementField.setAccessible(true);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    static void writeString(String string, DataOutput output) throws IOException {
        output.writeUTF(string);
    }

    static String readString(DataInput input) throws IOException {
        return input.readUTF();
    }

    static void writeLatin1String(String string, IgniteDataOutput output) throws IOException {
        byte[] bytes = StringIntrospection.fastLatin1Bytes(string);
        writeByteArray(bytes, output);
    }

    static String readLatin1String(IgniteDataInput input) throws IOException {
        int length = readLength(input);
        return input.materializeFromNextBytes(length, LATIN1_MATERIALIZER);
    }

    static Object readBareObject(@SuppressWarnings("unused") DataInput input) {
        return new Object();
    }

    static void writeUuid(UUID uuid, DataOutput output) throws IOException {
        output.writeLong(uuid.getMostSignificantBits());
        output.writeLong(uuid.getLeastSignificantBits());
    }

    static UUID readUuid(DataInput input) throws IOException {
        return new UUID(input.readLong(), input.readLong());
    }

    static void writeIgniteUuid(IgniteUuid uuid, DataOutput output) throws IOException {
        output.writeLong(uuid.localId());
        writeUuid(uuid.globalId(), output);
    }

    static IgniteUuid readIgniteUuid(DataInput input) throws IOException {
        long localId = input.readLong();
        UUID globalId = readUuid(input);
        return new IgniteUuid(globalId, localId);
    }

    static void writeDate(Date date, DataOutput output) throws IOException {
        output.writeLong(date.getTime());
    }

    static Date readDate(DataInput input) throws IOException {
        return new Date(input.readLong());
    }

    static void writeByteArray(byte[] array, IgniteDataOutput output) throws IOException {
        writeLength(array.length, output);
        output.writeByteArray(array);
    }

    static byte[] readByteArray(IgniteDataInput input) throws IOException {
        int length = readLength(input);
        return input.readByteArray(length);
    }

    static void writeShortArray(short[] array, IgniteDataOutput output) throws IOException {
        writeLength(array.length, output);
        output.writeShortArray(array);
    }

    static short[] readShortArray(IgniteDataInput input) throws IOException {
        int length = readLength(input);
        return input.readShortArray(length);
    }

    static void writeIntArray(int[] array, IgniteDataOutput output) throws IOException {
        writeLength(array.length, output);
        output.writeIntArray(array);
    }

    static int[] readIntArray(IgniteDataInput input) throws IOException {
        int length = readLength(input);
        return input.readIntArray(length);
    }

    static void writeFloatArray(float[] array, IgniteDataOutput output) throws IOException {
        writeLength(array.length, output);
        output.writeFloatArray(array);
    }

    static float[] readFloatArray(IgniteDataInput input) throws IOException {
        int length = readLength(input);
        return input.readFloatArray(length);
    }

    static void writeLongArray(long[] array, IgniteDataOutput output) throws IOException {
        writeLength(array.length, output);
        output.writeLongArray(array);
    }

    static long[] readLongArray(IgniteDataInput input) throws IOException {
        int length = readLength(input);
        return input.readLongArray(length);
    }

    static void writeDoubleArray(double[] array, IgniteDataOutput output) throws IOException {
        writeLength(array.length, output);
        output.writeDoubleArray(array);
    }

    static double[] readDoubleArray(IgniteDataInput input) throws IOException {
        int length = readLength(input);
        return input.readDoubleArray(length);
    }

    static void writeBooleanArray(boolean[] array, IgniteDataOutput output) throws IOException {
        writeLength(array.length, output);

        byte bits = 0;
        int writtenBytes = 0;
        for (int i = 0; i < array.length; i++) {
            boolean bit = array[i];
            int bitIndex = i % 8;
            if (bit) {
                bits |= (1 << bitIndex);
            }
            if (bitIndex == 7) {
                output.writeByte(bits);
                writtenBytes++;
                bits = 0;
            }
        }

        int totalBytesToWrite = numberOfBytesToPackBits(array.length);
        if (writtenBytes < totalBytesToWrite) {
            output.writeByte(bits);
        }
    }

    private static int numberOfBytesToPackBits(int length) {
        return length / 8 + (length % 8 == 0 ? 0 : 1);
    }

    static boolean[] readBooleanArray(IgniteDataInput input) throws IOException {
        int length = readLength(input);

        boolean[] array = new boolean[length];

        int totalBytesToRead = numberOfBytesToPackBits(length);

        for (int byteIndex = 0; byteIndex < totalBytesToRead; byteIndex++) {
            byte bits = input.readByte();

            int bitsToReadInThisByte;
            if (byteIndex < totalBytesToRead - 1) {
                bitsToReadInThisByte = 8;
            } else {
                bitsToReadInThisByte = length - (totalBytesToRead - 1) * 8;
            }
            for (int bitIndex = 0; bitIndex < bitsToReadInThisByte; bitIndex++) {
                if ((bits & (1 << bitIndex)) != 0) {
                    array[byteIndex * 8 + bitIndex] = true;
                }
            }
        }

        return array;
    }

    static void writeCharArray(char[] array, IgniteDataOutput output) throws IOException {
        writeLength(array.length, output);
        output.writeCharArray(array);
    }

    static char[] readCharArray(IgniteDataInput input) throws IOException {
        int length = readLength(input);
        return input.readCharArray(length);
    }

    static void writeBigDecimal(BigDecimal object, DataOutput output) throws IOException {
        writeString(object.toString(), output);
    }

    static BigDecimal readBigDecimal(DataInput input) throws IOException {
        return new BigDecimal(readString(input));
    }

    static void writeEnum(Enum<?> object, DataOutput output) throws IOException {
        writeString(object.name(), output);
    }

    static <T extends Enum<T>> Enum<T> readEnum(DataInput input, Class<T> enumClass) throws IOException {
        return Enum.valueOf(enumClass, readString(input));
    }

    private static <T> Class<T> classByName(String className, ClassLoader classLoader) throws UnmarshalException {
        try {
            @SuppressWarnings("unchecked") Class<T> castedClass = (Class<T>) Class.forName(className, true, classLoader);
            return castedClass;
        } catch (ClassNotFoundException e) {
            throw new UnmarshalException("Can not load a class: " + className, e);
        }
    }

    static void writeClass(Class<?> classToWrite, DataOutput output) throws IOException {
        writeString(classToWrite.getName(), output);
    }

    static Class<?> readClass(DataInput input, UnmarshallingContext context) throws IOException, UnmarshalException {
        String className = readString(input);
        return classByName(className, context.classLoader());
    }

    static void writeClassArray(Class<?>[] classes, IgniteDataOutput output, MarshallingContext context)
            throws IOException, MarshalException {
        writeRefArray(classes, output, classWriter, context);
    }

    static Class<?>[] readClassArray(IgniteDataInput input, UnmarshallingContext context) throws IOException, UnmarshalException {
        return readRefArray(input, classArrayFactory, classReader, context);
    }

    private static <T> void writeRefArray(T[] array, IgniteDataOutput output, ValueWriter<T> valueWriter, MarshallingContext context)
            throws IOException, MarshalException {
        writeLength(array.length, output);
        for (T object : array) {
            valueWriter.write(object, output, context);
        }
    }

    private static <T> T[] readRefArray(
            IgniteDataInput input,
            IntFunction<T[]> arrayFactory,
            ValueReader<T> valueReader,
            UnmarshallingContext context
    ) throws IOException, UnmarshalException {
        int length = readLength(input);

        T[] array = arrayFactory.apply(length);
        fillRefArrayFrom(input, array, valueReader, context);

        return array;
    }

    private static <T> void fillRefArrayFrom(IgniteDataInput input, T[] array, ValueReader<T> valueReader, UnmarshallingContext context)
            throws IOException, UnmarshalException {
        for (int i = 0; i < array.length; i++) {
            array[i] = valueReader.read(input, context);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> IntFunction<T[]> readTypeAndCreateArrayFactory(DataInput input, UnmarshallingContext context)
            throws IOException, UnmarshalException {
        Class<T> componentType = (Class<T>) readClass(input, context);
        return len -> (T[]) Array.newInstance(componentType, len);
    }

    static <T> T[] preInstantiateGenericRefArray(DataInput input, UnmarshallingContext context) throws IOException, UnmarshalException {
        IntFunction<T[]> arrayFactory = readTypeAndCreateArrayFactory(input, context);
        int length = readLength(input);
        return arrayFactory.apply(length);
    }

    static <T> void writeCollection(
            Collection<T> collection,
            IgniteDataOutput output,
            ValueWriter<T> valueWriter,
            MarshallingContext context
    ) throws IOException, MarshalException {
        writeLength(collection.size(), output);

        if (collection instanceof List && collection instanceof RandomAccess) {
            writeRandomAccessListElements(output, valueWriter, context, (List<T>) collection);
        } else {
            for (T object : collection) {
                valueWriter.write(object, output, context);
            }
        }
    }

    private static <T> void writeRandomAccessListElements(
            IgniteDataOutput output,
            ValueWriter<T> valueWriter,
            MarshallingContext context,
            List<T> list
    ) throws IOException, MarshalException {
        //noinspection ForLoopReplaceableByForEach
        for (int i = 0; i < list.size(); i++) {
            valueWriter.write(list.get(i), output, context);
        }
    }

    static <T, C extends Collection<T>> void fillCollectionFrom(
            IgniteDataInput input,
            C collection,
            ValueReader<T> valueReader,
            UnmarshallingContext context
    ) throws IOException, UnmarshalException {
        int length = readLength(input);

        for (int i = 0; i < length; i++) {
            collection.add(valueReader.read(input, context));
        }
    }

    static <T, C extends Collection<T>> C preInstantiateCollection(DataInput input, IntFunction<C> collectionFactory) throws IOException {
        int length = readLength(input);
        return collectionFactory.apply(length);
    }

    static <T, C extends Collection<T>> void fillSingletonCollectionFrom(
            IgniteDataInput input,
            C collection,
            ValueReader<T> elementReader,
            UnmarshallingContext context
    ) throws IOException, UnmarshalException {
        T element = elementReader.read(input, context);

        try {
            singletonListElementField.set(collection, element);
        } catch (ReflectiveOperationException e) {
            throw new UnmarshalException("Cannot set field value", e);
        }
    }

    static <K, V> void writeMap(
            Map<K, V> map,
            IgniteDataOutput output,
            ValueWriter<K> keyWriter,
            ValueWriter<V> valueWriter,
            MarshallingContext context
    ) throws IOException, MarshalException {
        writeLength(map.size(), output);

        for (Map.Entry<K, V> entry : map.entrySet()) {
            keyWriter.write(entry.getKey(), output, context);
            valueWriter.write(entry.getValue(), output, context);
        }
    }

    static <K, V, M extends Map<K, V>> void fillMapFrom(
            IgniteDataInput input,
            M map,
            ValueReader<K> keyReader,
            ValueReader<V> valueReader,
            UnmarshallingContext context
    ) throws IOException, UnmarshalException {
        int length = readLength(input);

        for (int i = 0; i < length; i++) {
            map.put(keyReader.read(input, context), valueReader.read(input, context));
        }
    }

    static <K, V, M extends Map<K, V>> M preInstantiateMap(DataInput input, IntFunction<M> mapFactory) throws IOException {
        int length = readLength(input);
        return mapFactory.apply(length);
    }

    static void writeBitSet(BitSet object, IgniteDataOutput output) throws IOException {
        writeByteArray(object.toByteArray(), output);
    }

    static BitSet readBitSet(IgniteDataInput input) throws IOException {
        return BitSet.valueOf(readByteArray(input));
    }

    private BuiltInMarshalling() {
    }
}
