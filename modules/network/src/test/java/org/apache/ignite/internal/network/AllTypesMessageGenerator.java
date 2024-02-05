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

package org.apache.ignite.internal.network;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;
import java.util.Random;
import java.util.UUID;
import java.util.stream.IntStream;
import org.apache.ignite.internal.lang.IgniteUuid;
import org.apache.ignite.internal.lang.IgniteUuidGenerator;
import org.apache.ignite.internal.network.messages.AllTypesMessage;
import org.apache.ignite.internal.network.messages.AllTypesMessageBuilder;
import org.apache.ignite.internal.network.messages.AllTypesMessageImpl;
import org.apache.ignite.internal.network.messages.TestMessagesFactory;
import org.jetbrains.annotations.Nullable;

/**
 * Generator for an {@link AllTypesMessage}.
 */
public class AllTypesMessageGenerator {
    public static AllTypesMessage generate(long seed, boolean nestedMsg) {
        return generate(seed, nestedMsg, true);
    }

    /**
     * Generate a new {@link AllTypesMessage}.
     *
     * @param seed      Random seed.
     * @param nestedMsg {@code true} if nested messages should be generated, {@code false} otherwise.
     * @return Message.
     */
    public static AllTypesMessage generate(long seed, boolean nestedMsg, boolean fillArrays) {
        try {
            var random = new Random(seed);

            AllTypesMessageBuilder message = new TestMessagesFactory().allTypesMessage();

            Field[] fields = AllTypesMessageImpl.builder().getClass().getDeclaredFields();

            for (Field field : fields) {
                field.setAccessible(true);

                if (!field.getType().isArray() || fillArrays) {
                    field.set(message, randomValue(random, field, nestedMsg));
                }
            }

            if (nestedMsg) {
                message.netMsgArrV(
                        IntStream.range(0, 10).mapToObj(unused -> generate(seed, false, fillArrays)).toArray(NetworkMessage[]::new)
                );

                message.netMsgCollW(IntStream.range(0, 10)
                        .mapToObj(unused -> generate(seed, false, fillArrays))
                        .collect(toList()));

                message.netMsgMapX(IntStream.range(0, 10)
                        .boxed()
                        .collect(toMap(String::valueOf, unused -> generate(seed, false, fillArrays))));

                message.netMsgListY(IntStream.range(0, 10)
                        .mapToObj(unused -> generate(seed, false, fillArrays))
                        .collect(toList()));

                message.netMsgSetY(IntStream.range(0, 10)
                        .mapToObj(unused -> generate(seed, false, fillArrays))
                        .collect(toSet()));
            }

            return message.build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Generate random value.
     *
     * @param random    Seeded random.
     * @param field     Field which value is being generated.
     * @param nestedMsg {@code true} if nested messages should be generated, {@code false} otherwise.
     * @return Random value.
     */
    @Nullable
    private static Object randomValue(Random random, Field field, boolean nestedMsg) {
        if (field.isAnnotationPresent(Nullable.class) && random.nextBoolean()) {
            return null;
        }

        Class<?> type = field.getType();

        if (type == byte.class || type == Byte.class) {
            return (byte) random.nextInt();
        } else if (type == short.class || type == Short.class) {
            return (short) random.nextInt();
        } else if (type == int.class || type == Integer.class) {
            return random.nextInt();
        } else if (type == long.class || type == Long.class) {
            return random.nextLong();
        } else if (type == float.class || type == Float.class) {
            return random.nextFloat();
        } else if (type == double.class || type == Double.class) {
            return random.nextDouble();
        } else if (type == char.class || type == Character.class) {
            return (char) random.nextInt();
        } else if (type == boolean.class || type == Boolean.class) {
            return random.nextBoolean();
        } else if (type == byte[].class) {
            int byteArrLen = random.nextInt(1024);
            byte[] bytes = new byte[byteArrLen];
            random.nextBytes(bytes);
            return bytes;
        } else if (type == short[].class) {
            int shortArrLen = random.nextInt(1024);
            short[] shorts = new short[1024];
            for (int i = 0; i < shortArrLen; i++) {
                shorts[i] = (short) random.nextInt();
            }
            return shorts;
        } else if (type == int[].class) {
            int intArrLen = random.nextInt(1024);
            int[] ints = new int[1024];
            for (int i = 0; i < intArrLen; i++) {
                ints[i] = random.nextInt();
            }
            return ints;
        } else if (type == long[].class) {
            int longArrLen = random.nextInt(1024);
            long[] longs = new long[1024];
            for (int i = 0; i < longArrLen; i++) {
                longs[i] = random.nextLong();
            }
            return longs;
        } else if (type == float[].class) {
            int floatArrLen = random.nextInt(1024);
            float[] floats = new float[1024];
            for (int i = 0; i < floatArrLen; i++) {
                floats[i] = random.nextFloat();
            }
            return floats;
        } else if (type == double[].class) {
            int doubleArrLen = random.nextInt(1024);
            double[] doubles = new double[1024];
            for (int i = 0; i < doubleArrLen; i++) {
                doubles[i] = random.nextDouble();
            }
            return doubles;
        } else if (type == char[].class) {
            int charArrLen = random.nextInt(1024);
            char[] chars = new char[1024];
            for (int i = 0; i < charArrLen; i++) {
                chars[i] = (char) random.nextInt();
            }
            return chars;
        } else if (type == boolean[].class) {
            int booleanArrLen = random.nextInt(1024);
            boolean[] booleans = new boolean[1024];
            for (int i = 0; i < booleanArrLen; i++) {
                booleans[i] = random.nextBoolean();
            }
            return booleans;
        } else if (type == String.class) {
            int l = 'a';
            int strLen = random.nextInt(1024);
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < strLen; i++) {
                int letter = l + random.nextInt(26);
                sb.append(letter);
            }
            return sb.toString();
        } else if (type == BitSet.class) {
            BitSet set = new BitSet();
            int setLen = random.nextInt(10);
            for (int i = 0; i < setLen; i++) {
                if (random.nextBoolean()) {
                    set.set(i);
                }
            }
            return set;
        } else if (type == UUID.class) {
            byte[] uuidBytes = new byte[16];
            random.nextBytes(uuidBytes);
            return UUID.nameUUIDFromBytes(uuidBytes);
        } else if (type == IgniteUuid.class) {
            byte[] igniteUuidBytes = new byte[16];
            random.nextBytes(igniteUuidBytes);
            var generator = new IgniteUuidGenerator(UUID.nameUUIDFromBytes(igniteUuidBytes), 0);
            return generator.randomUuid();
        } else if (NetworkMessage.class.isAssignableFrom(type)) {
            if (nestedMsg) {
                return generate(random.nextLong(), false);
            }
            return null;
        } else if (type == ByteBuffer.class) {
            byte[] bytes = new byte[16];
            random.nextBytes(bytes);

            ByteBuffer outerBuffer = random.nextBoolean() ? ByteBuffer.allocate(20) : ByteBuffer.allocateDirect(20);
            ByteBuffer buffer = outerBuffer.position(2).limit(18).slice();
            buffer.put(bytes);

            buffer.order(random.nextBoolean() ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
            buffer.position(random.nextInt(3));
            buffer.limit(14 + random.nextInt(3));

            return buffer;
        } else {
            return null;
        }
    }
}
