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

package org.apache.ignite.internal.configuration.util;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Utility class to help serializing  deserializing configuration values - primitives, strings, or arrays of primitives or strings.
 */
public class ConfigurationSerializationUtil {
    private static final byte BOOLEAN = 1;

    private static final byte BYTE = 2;

    private static final byte SHORT = 3;

    private static final byte INT = 4;

    private static final byte LONG = 5;

    private static final byte CHAR = 6;

    private static final byte FLOAT = 7;

    private static final byte DOUBLE = 8;

    private static final byte STRING = 9;

    private static final byte ARRAY = (byte) 0x80;
    private static final String[] EMPTY_STRING_ARRAY = new String[0];

    /**
     * Converts configuration value into a byte array.
     *
     * @param value Configuration values.
     * @return Serialized value.
     */
    public static byte[] toBytes(Serializable value) {
        Objects.requireNonNull(value);

        int header = header(value.getClass());

        switch (header) {
            case BOOLEAN:
                return new byte[]{BOOLEAN, (byte) ((boolean) value ? 1 : 0)};

            case BYTE:
                return new byte[]{BYTE, (byte) value};

            case SHORT:
                return allocateBuffer(3).put(SHORT).putShort((short) value).array();

            case INT:
                return allocateBuffer(5).put(INT).putInt((int) value).array();

            case LONG:
                return allocateBuffer(9).put(LONG).putLong((long) value).array();

            case CHAR:
                return allocateBuffer(3).put(CHAR).putChar((char) value).array();

            case FLOAT:
                return allocateBuffer(5).put(FLOAT).putFloat((float) value).array();

            case DOUBLE:
                return allocateBuffer(9).put(DOUBLE).putDouble((double) value).array();

            case STRING: {
                byte[] strBytes = ((String) value).getBytes(StandardCharsets.UTF_8);

                return allocateBuffer(1 + strBytes.length).put(STRING).put(strBytes).array();
            }

            case BOOLEAN | ARRAY: {
                boolean[] booleans = (boolean[]) value;

                ByteBuffer buf = allocateBuffer(1 + booleans.length);

                buf.put((byte) (BOOLEAN | ARRAY));

                for (boolean b : booleans) {
                    buf.put((byte) (b ? 1 : 0));
                }

                return buf.array();
            }

            case BYTE | ARRAY: {
                byte[] bytes = (byte[]) value;

                return allocateBuffer(1 + bytes.length).put((byte) (BYTE | ARRAY)).put(bytes).array();
            }

            case SHORT | ARRAY: {
                short[] shorts = (short[]) value;

                ByteBuffer buf = allocateBuffer(1 + 2 * shorts.length);

                buf.put((byte) (SHORT | ARRAY));

                for (short s : shorts) {
                    buf.putShort(s);
                }

                return buf.array();
            }

            case INT | ARRAY: {
                int[] ints = (int[]) value;

                ByteBuffer buf = allocateBuffer(1 + 4 * ints.length);

                buf.put((byte) (INT | ARRAY));

                for (int n : ints) {
                    buf.putInt(n);
                }

                return buf.array();
            }

            case LONG | ARRAY: {
                long[] longs = (long[]) value;

                ByteBuffer buf = allocateBuffer(1 + 8 * longs.length);

                buf.put((byte) (LONG | ARRAY));

                for (long n : longs) {
                    buf.putLong(n);
                }

                return buf.array();
            }

            case CHAR | ARRAY: {
                char[] chars = (char[]) value;

                ByteBuffer buf = allocateBuffer(1 + 2 * chars.length);

                buf.put((byte) (CHAR | ARRAY));

                for (char c : chars) {
                    buf.putChar(c);
                }

                return buf.array();
            }

            case FLOAT | ARRAY: {
                float[] floats = (float[]) value;

                ByteBuffer buf = allocateBuffer(1 + 4 * floats.length);

                buf.put((byte) (FLOAT | ARRAY));

                for (float f : floats) {
                    buf.putFloat(f);
                }

                return buf.array();
            }

            case DOUBLE | ARRAY: {
                double[] doubles = (double[]) value;

                ByteBuffer buf = allocateBuffer(1 + 8 * doubles.length);

                buf.put((byte) (DOUBLE | ARRAY));

                for (double d : doubles) {
                    buf.putDouble(d);
                }

                return buf.array();
            }

            case STRING | ARRAY: {
                String[] strings = (String[]) value;

                byte[][] strBytes = Arrays.stream(strings).map(s -> s.getBytes(StandardCharsets.UTF_8)).toArray(byte[][]::new);

                int totalSize = Arrays.stream(strBytes).mapToInt(bytes -> bytes.length).sum();

                ByteBuffer buf = allocateBuffer(1 + 4 * strBytes.length + totalSize);

                buf.put((byte) (STRING | ARRAY));

                for (int i = 0; i < strings.length; i++) {
                    buf.putInt(strings[i].length());

                    buf.put(strBytes[i]);
                }

                return buf.array();
            }

            default:
                throw new IllegalStateException(value.getClass().getName());
        }
    }

    /**
     * Converts byte array into a configuration value.
     *
     * @param bytes Serialized value.
     * @return Deserialized configuration value.
     */
    public static Serializable fromBytes(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);

        buf.order(ByteOrder.BIG_ENDIAN);

        byte header = buf.get();

        switch (header) {
            case BOOLEAN:
                return buf.get() == 1;

            case BYTE:
                return buf.get();

            case SHORT:
                return buf.getShort();

            case INT:
                return buf.getInt();

            case LONG:
                return buf.getLong();

            case CHAR:
                return buf.getChar();

            case FLOAT:
                return buf.getFloat();

            case DOUBLE:
                return buf.getDouble();

            case STRING:
                return new String(bytes, 1, bytes.length - 1, StandardCharsets.UTF_8);

            case BOOLEAN | ARRAY: {
                boolean[] booleans = new boolean[bytes.length - 1];

                for (int i = 0; i < booleans.length; i++) {
                    booleans[i] = buf.get() == 1;
                }

                return booleans;
            }

            case BYTE | ARRAY:
                return Arrays.copyOfRange(bytes, 1, bytes.length);

            case SHORT | ARRAY: {
                short[] shorts = new short[bytes.length / 2];

                for (int i = 0; i < shorts.length; i++) {
                    shorts[i] = buf.getShort();
                }

                return shorts;
            }

            case INT | ARRAY: {
                int[] ints = new int[bytes.length / 4];

                for (int i = 0; i < ints.length; i++) {
                    ints[i] = buf.getInt();
                }

                return ints;
            }

            case LONG | ARRAY: {
                long[] longs = new long[bytes.length / 8];

                for (int i = 0; i < longs.length; i++) {
                    longs[i] = buf.getLong();
                }

                return longs;
            }

            case CHAR | ARRAY: {
                char[] chars = new char[bytes.length / 2];

                for (int i = 0; i < chars.length; i++) {
                    chars[i] = buf.getChar();
                }

                return chars;
            }

            case FLOAT | ARRAY: {
                float[] floats = new float[bytes.length / 4];

                for (int i = 0; i < floats.length; i++) {
                    floats[i] = buf.getFloat();
                }

                return floats;
            }

            case DOUBLE | ARRAY: {
                double[] doubles = new double[bytes.length / 8];

                for (int i = 0; i < doubles.length; i++) {
                    doubles[i] = buf.getDouble();
                }

                return doubles;
            }

            case STRING | ARRAY: {
                List<String> res = new ArrayList<>();

                int offset = 1;

                while (offset != bytes.length) {
                    int size = buf.getInt(offset);

                    res.add(new String(bytes, offset + 4, size));

                    offset += 4 + size;
                }

                return res.toArray(EMPTY_STRING_ARRAY);
            }

            default:
                throw new IllegalArgumentException(Arrays.toString(bytes));
        }
    }

    private static ByteBuffer allocateBuffer(int size) {
        ByteBuffer buf = ByteBuffer.allocate(size);

        buf.order(ByteOrder.BIG_ENDIAN);

        return buf;
    }

    private static int header(Class<?> clazz) {
        if (clazz == Boolean.class) {
            return BOOLEAN;
        } else if (clazz == Byte.class) {
            return BYTE;
        } else if (clazz == Short.class) {
            return SHORT;
        } else if (clazz == Integer.class) {
            return INT;
        } else if (clazz == Long.class) {
            return LONG;
        } else if (clazz == Character.class) {
            return CHAR;
        } else if (clazz == Float.class) {
            return FLOAT;
        } else if (clazz == Double.class) {
            return DOUBLE;
        } else if (clazz == String.class) {
            return STRING;
        } else if (clazz == boolean[].class) {
            return BOOLEAN | ARRAY;
        } else if (clazz == byte[].class) {
            return BYTE | ARRAY;
        } else if (clazz == short[].class) {
            return SHORT | ARRAY;
        } else if (clazz == int[].class) {
            return INT | ARRAY;
        } else if (clazz == long[].class) {
            return LONG | ARRAY;
        } else if (clazz == char[].class) {
            return CHAR | ARRAY;
        } else if (clazz == float[].class) {
            return FLOAT | ARRAY;
        } else if (clazz == double[].class) {
            return DOUBLE | ARRAY;
        } else if (clazz == String[].class) {
            return STRING | ARRAY;
        } else {
            throw new IllegalArgumentException(clazz.getName());
        }
    }
}
