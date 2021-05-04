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

package org.apache.ignite.network.internal;

import java.lang.reflect.Field;
import java.util.BitSet;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.lang.IgniteUuidGenerator;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;

/**
 * Generator for an {@link AllTypesMessage}.
 */
public class AllTypesMessageGenerator {
    /**
     * Generate a new {@link AllTypesMessage}.
     *
     * @param seed Random seed.
     * @param nestedMsg {@code true} if nested messages should be generated, {@code false} otherwise.
     * @return Message.
     * @throws Exception If failed.
     */
    public static AllTypesMessage generate(long seed, boolean nestedMsg) throws Exception {
        var random = new Random(seed);

        var message = new AllTypesMessage();

        Field[] fields = AllTypesMessage.class.getDeclaredFields();

        for (Field field : fields) {
            field.setAccessible(true);

            TestFieldType annotation = field.getAnnotation(TestFieldType.class);

            if (annotation != null) {
                field.set(message, randomValue(random, annotation.value(), nestedMsg));
            }
        }

        Field objectArrayField = AllTypesMessage.class.getDeclaredField("v");
        Field collectionField = AllTypesMessage.class.getDeclaredField("w");
        Field mapField = AllTypesMessage.class.getDeclaredField("x");

        return message;
    }

    /**
     * Generate random value.
     *
     * @param random Seeded random.
     * @param type Value type.
     * @param nestedMsg {@code true} if nested messages should be generated, {@code false} otherwise.
     * @return Random value.
     * @throws Exception If failed.
     */
    private static Object randomValue(Random random, MessageCollectionItemType type, boolean nestedMsg) throws Exception {
        Object value = null;

        switch (type) {
            case BYTE:
                value = (byte) random.nextInt();
                break;

            case SHORT:
                value = (short) random.nextInt();
                break;

            case INT:
                value = random.nextInt();
                break;

            case LONG:
                value = random.nextLong();
                break;

            case FLOAT:
                value = random.nextFloat();
                break;

            case DOUBLE:
                value = random.nextDouble();
                break;

            case CHAR:
                value = (char) random.nextInt();
                break;

            case BOOLEAN:
                value = random.nextBoolean();
                break;

            case BYTE_ARR:
                int byteArrLen = random.nextInt(1024);
                byte[] bytes = new byte[byteArrLen];
                random.nextBytes(bytes);
                value = bytes;
                break;

            case SHORT_ARR:
                int shortArrLen = random.nextInt(1024);
                short[] shorts = new short[1024];
                for (int i = 0; i < shortArrLen; i++) {
                    shorts[i] = (short) random.nextInt();
                }
                value = shorts;
                break;

            case INT_ARR:
                int intArrLen = random.nextInt(1024);
                int[] ints = new int[1024];
                for (int i = 0; i < intArrLen; i++) {
                    ints[i] = random.nextInt();
                }
                value = ints;
                break;

            case LONG_ARR:
                int longArrLen = random.nextInt(1024);
                long[] longs = new long[1024];
                for (int i = 0; i < longArrLen; i++) {
                    longs[i] = random.nextLong();
                }
                value = longs;
                break;

            case FLOAT_ARR:
                int floatArrLen = random.nextInt(1024);
                float[] floats = new float[1024];
                for (int i = 0; i < floatArrLen; i++) {
                    floats[i] = random.nextFloat();
                }
                value = floats;
                break;

            case DOUBLE_ARR:
                int doubleArrLen = random.nextInt(1024);
                double[] doubles = new double[1024];
                for (int i = 0; i < doubleArrLen; i++) {
                    doubles[i] = random.nextDouble();
                }
                value = doubles;
                break;

            case CHAR_ARR:
                int charArrLen = random.nextInt(1024);
                char[] chars = new char[1024];
                for (int i = 0; i < charArrLen; i++) {
                    chars[i] = (char) random.nextInt();
                }
                value = chars;
                break;

            case BOOLEAN_ARR:
                int booleanArrLen = random.nextInt(1024);
                boolean[] booleans = new boolean[1024];
                for (int i = 0; i < booleanArrLen; i++) {
                    booleans[i] = random.nextBoolean();
                }
                value = booleans;
                break;

            case STRING:
                int aLetter = 'a';
                int strLen = random.nextInt(1024);
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < strLen; i++) {
                    int letter = aLetter + random.nextInt(26);
                    sb.append(letter);
                }
                value = sb.toString();
                break;

            case BIT_SET:
                BitSet set = new BitSet();
                int setLen = random.nextInt(10);
                for (int i = 0; i < setLen; i++) {
                    if (random.nextBoolean()) {
                        set.set(i);
                    }
                }

                value = set;
                break;

            case UUID:
                byte[] uuidBytes = new byte[16];
                random.nextBytes(uuidBytes);
                value = UUID.nameUUIDFromBytes(uuidBytes);
                break;

            case IGNITE_UUID:
                byte[] igniteUuidBytes = new byte[16];
                random.nextBytes(igniteUuidBytes);
                var generator = new IgniteUuidGenerator(UUID.nameUUIDFromBytes(igniteUuidBytes), 0);
                value = generator.randomUuid();
                break;

            case MSG:
                if (nestedMsg) {
                    value = generate(random.nextLong(), false);
                }
                break;

        }

        return value;
    }
}
