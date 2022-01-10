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

import static java.util.Collections.singletonList;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.BitSet;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.function.IntFunction;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.NotNull;

/**
 * Built-in types marshalling.
 */
class BuiltInMarshalling {
    private static final ValueWriter<String> stringWriter = (obj, out, ctx) -> writeString(obj, out);
    private static final IntFunction<String[]> stringArrayFactory = String[]::new;
    private static final ValueReader<String> stringReader = (in, ctx) -> readString(in);

    private static final ValueWriter<BigDecimal> bigDecimalWriter = (obj, out, ctx) -> writeBigDecimal(obj, out);
    private static final IntFunction<BigDecimal[]> bigDecimalArrayFactory = BigDecimal[]::new;
    private static final ValueReader<BigDecimal> bigDecimalReader = (in, ctx) -> readBigDecimal(in);

    private static final ValueWriter<Enum<?>> enumWriter = (obj, out, ctx) -> writeEnum(obj, out);
    private static final ValueReader<Enum<?>> enumReader = (in, ctx) -> readEnum(in);

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

    static void writeByteArray(byte[] array, DataOutput output) throws IOException {
        output.writeInt(array.length);
        for (byte b : array) {
            output.writeByte(b);
        }
    }

    static byte[] readByteArray(DataInput input) throws IOException {
        int length = input.readInt();
        byte[] array = new byte[length];
        for (int i = 0; i < length; i++) {
            array[i] = input.readByte();
        }
        return array;
    }

    static void writeShortArray(short[] array, DataOutput output) throws IOException {
        output.writeInt(array.length);
        for (short sh : array) {
            output.writeShort(sh);
        }
    }

    static short[] readShortArray(DataInput input) throws IOException {
        int length = input.readInt();
        short[] array = new short[length];
        for (int i = 0; i < length; i++) {
            array[i] = input.readShort();
        }
        return array;
    }

    static void writeIntArray(int[] array, DataOutput output) throws IOException {
        output.writeInt(array.length);
        for (int sh : array) {
            output.writeInt(sh);
        }
    }

    static int[] readIntArray(DataInput input) throws IOException {
        int length = input.readInt();
        int[] array = new int[length];
        for (int i = 0; i < length; i++) {
            array[i] = input.readInt();
        }
        return array;
    }

    static void writeFloatArray(float[] array, DataOutput output) throws IOException {
        output.writeInt(array.length);
        for (float sh : array) {
            output.writeFloat(sh);
        }
    }

    static float[] readFloatArray(DataInput input) throws IOException {
        int length = input.readInt();
        float[] array = new float[length];
        for (int i = 0; i < length; i++) {
            array[i] = input.readFloat();
        }
        return array;
    }

    static void writeLongArray(long[] array, DataOutput output) throws IOException {
        output.writeInt(array.length);
        for (long sh : array) {
            output.writeLong(sh);
        }
    }

    static long[] readLongArray(DataInput input) throws IOException {
        int length = input.readInt();
        long[] array = new long[length];
        for (int i = 0; i < length; i++) {
            array[i] = input.readLong();
        }
        return array;
    }

    static void writeDoubleArray(double[] array, DataOutput output) throws IOException {
        output.writeInt(array.length);
        for (double sh : array) {
            output.writeDouble(sh);
        }
    }

    static double[] readDoubleArray(DataInput input) throws IOException {
        int length = input.readInt();
        double[] array = new double[length];
        for (int i = 0; i < length; i++) {
            array[i] = input.readDouble();
        }
        return array;
    }

    static void writeBooleanArray(boolean[] array, DataOutput output) throws IOException {
        output.writeInt(array.length);
        for (boolean sh : array) {
            output.writeBoolean(sh);
        }
    }

    static boolean[] readBooleanArray(DataInput input) throws IOException {
        int length = input.readInt();
        boolean[] array = new boolean[length];
        for (int i = 0; i < length; i++) {
            array[i] = input.readBoolean();
        }
        return array;
    }

    static void writeCharArray(char[] array, DataOutput output) throws IOException {
        output.writeInt(array.length);
        for (char sh : array) {
            output.writeChar(sh);
        }
    }

    static char[] readCharArray(DataInput input) throws IOException {
        int length = input.readInt();
        char[] array = new char[length];
        for (int i = 0; i < length; i++) {
            array[i] = input.readChar();
        }
        return array;
    }

    static void writeBigDecimal(BigDecimal object, DataOutput output) throws IOException {
        output.writeUTF(object.toString());
    }

    static BigDecimal readBigDecimal(DataInput input) throws IOException {
        return new BigDecimal(input.readUTF());
    }

    static void writeEnum(Enum<?> object, DataOutput output) throws IOException {
        Class<?> enumClass = object.getClass();
        if (!enumClass.isEnum()) {
            // this is needed for enums where members are represented with anonymous classes
            enumClass = enumClass.getSuperclass();
        }

        assert enumClass.getSuperclass() == Enum.class;

        output.writeUTF(enumClass.getName());
        output.writeUTF(object.name());
    }

    static <T extends Enum<T>> Enum<?> readEnum(DataInput input) throws IOException {
        String enumClassName = input.readUTF();
        Class<T> enumClass = enumClass(enumClassName);
        return Enum.valueOf(enumClass, input.readUTF());
    }

    private static <T extends Enum<T>> Class<T> enumClass(String className) throws IOException {
        return classByName(className, "enum");
    }

    @NotNull
    private static <T> Class<T> classByName(String className, String classKind) throws IOException {
        try {
            // TODO: what classloader to use?
            @SuppressWarnings("unchecked") Class<T> castedClass = (Class<T>) Class.forName(className);
            return castedClass;
        } catch (ClassNotFoundException e) {
            throw new IOException("Can not load " + classKind + " class: " + className, e);
        }
    }

    static <T> void writeRefArray(T[] array, DataOutput output, ValueWriter<T> valueWriter, MarshallingContext context)
            throws IOException, MarshalException {
        output.writeInt(array.length);
        for (T object : array) {
            valueWriter.write(object, output, context);
        }
    }

    static <T> T[] readRefArray(DataInput input, IntFunction<T[]> arrayFactory, ValueReader<T> valueReader, UnmarshallingContext context)
            throws IOException, UnmarshalException {
        int length = input.readInt();

        T[] array = arrayFactory.apply(length);
        fillRefArrayFrom(input, array, valueReader, context);

        return array;
    }

    private static <T> void fillRefArrayFrom(DataInput input, T[] array, ValueReader<T> valueReader, UnmarshallingContext context)
            throws IOException, UnmarshalException {
        for (int i = 0; i < array.length; i++) {
            array[i] = valueReader.read(input, context);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> IntFunction<T[]> readTypeAndCreateArrayFactory(DataInput input) throws IOException {
        String componentClassName = input.readUTF();
        Class<T> componentType = classByName(componentClassName, "component");
        return len -> (T[]) Array.newInstance(componentType, len);
    }

    static <T> T[] preInstantiateGenericRefArray(DataInput input) throws IOException {
        IntFunction<T[]> arrayFactory = readTypeAndCreateArrayFactory(input);
        int length = input.readInt();
        return arrayFactory.apply(length);
    }

    static <T> void fillGenericRefArray(DataInput input, T[] array, ValueReader<T> elementReader, UnmarshallingContext context)
            throws IOException, UnmarshalException {
        fillRefArrayFrom(input, array, elementReader, context);
    }

    static void writeStringArray(String[] array, DataOutput output, MarshallingContext context) throws IOException, MarshalException {
        writeRefArray(array, output, stringWriter, context);
    }

    static String[] readStringArray(DataInput input, UnmarshallingContext context) throws IOException, UnmarshalException {
        return readRefArray(input, stringArrayFactory, stringReader, context);
    }

    static void writeBigDecimalArray(BigDecimal[] array, DataOutput output, MarshallingContext context)
            throws IOException, MarshalException {
        writeRefArray(array, output, bigDecimalWriter, context);
    }

    static BigDecimal[] readBigDecimalArray(DataInput input, UnmarshallingContext context) throws IOException, UnmarshalException {
        return readRefArray(input, bigDecimalArrayFactory, bigDecimalReader, context);
    }

    static void writeEnumArray(Enum<?>[] array, DataOutput output, MarshallingContext context) throws IOException, MarshalException {
        output.writeUTF(array.getClass().getComponentType().getName());
        writeRefArray(array, output, enumWriter, context);
    }

    static Enum<?>[] readEnumArray(DataInput input, UnmarshallingContext context) throws IOException, UnmarshalException {
        String enumClassName = input.readUTF();
        Class<? extends Enum<?>> enumClass = enumClass(enumClassName);
        return readRefArray(input, len -> (Enum<?>[]) Array.newInstance(enumClass, len), enumReader, context);
    }

    static <T> void writeCollection(Collection<T> collection, DataOutput output, ValueWriter<T> valueWriter, MarshallingContext context)
            throws IOException, MarshalException {
        output.writeInt(collection.size());

        for (T object : collection) {
            valueWriter.write(object, output, context);
        }
    }

    static <T, C extends Collection<T>> void fillCollectionFrom(
            DataInput input,
            C collection,
            ValueReader<T> valueReader,
            UnmarshallingContext context
    ) throws IOException, UnmarshalException {
        int length = input.readInt();

        for (int i = 0; i < length; i++) {
            collection.add(valueReader.read(input, context));
        }
    }

    static <T, C extends Collection<T>> C preInstantiateCollection(DataInput input, IntFunction<C> collectionFactory) throws IOException {
        int length = input.readInt();
        return collectionFactory.apply(length);
    }

    static <T, C extends Collection<T>> void fillSingletonCollectionFrom(
            DataInput input,
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
            DataOutput output,
            ValueWriter<K> keyWriter,
            ValueWriter<V> valueWriter,
            MarshallingContext context
    ) throws IOException, MarshalException {
        output.writeInt(map.size());

        for (Map.Entry<K, V> entry : map.entrySet()) {
            keyWriter.write(entry.getKey(), output, context);
            valueWriter.write(entry.getValue(), output, context);
        }
    }

    static <K, V, M extends Map<K, V>> void fillMapFrom(
            DataInput input,
            M map,
            ValueReader<K> keyReader,
            ValueReader<V> valueReader,
            UnmarshallingContext context
    ) throws IOException, UnmarshalException {
        int length = input.readInt();
        for (int i = 0; i < length; i++) {
            map.put(keyReader.read(input, context), valueReader.read(input, context));
        }
    }

    static <K, V, M extends Map<K, V>> M preInstantiateMap(DataInput input, IntFunction<M> mapFactory) throws IOException {
        int length = input.readInt();
        return mapFactory.apply(length);
    }

    static void writeBitSet(BitSet object, DataOutput output) throws IOException {
        writeByteArray(object.toByteArray(), output);
    }

    static BitSet readBitSet(DataInput input) throws IOException {
        return BitSet.valueOf(readByteArray(input));
    }

    private BuiltInMarshalling() {
    }
}
