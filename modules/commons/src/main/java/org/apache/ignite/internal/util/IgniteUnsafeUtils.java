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

package org.apache.ignite.internal.util;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import sun.misc.Unsafe;

/**
 * Unsafe utility.
 */
//TODO Move class to 'java-8' profile. Java9+ should use varhandles instead.
public final class IgniteUnsafeUtils {
    /** Unsafe. */
    private static final Unsafe UNSAFE = unsafe();

    /**
     * @return Instance of Unsafe class.
     */
    private static Unsafe unsafe() {
        try {
            return Unsafe.getUnsafe();
        }
        catch (SecurityException ignored) {
            try {
                return AccessController.doPrivileged(
                    new PrivilegedExceptionAction<Unsafe>() {
                        @Override public Unsafe run() throws Exception {
                            Field f = Unsafe.class.getDeclaredField("theUnsafe");

                            f.setAccessible(true);

                            return (Unsafe)f.get(null);
                        }
                    });
            }
            catch (PrivilegedActionException e) {
                throw new RuntimeException("Could not initialize intrinsics.", e.getCause());
            }
        }
    }

    /**
     * Returns object field offset.
     *
     * @param field Field.
     * @return Object field offset.
     */
    public static long objectFieldOffset(Field field) {
        return UNSAFE.objectFieldOffset(field);
    }

    /**
     * Gets boolean value from object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @return Boolean value from object field.
     */
    public static boolean getBooleanField(Object obj, long fieldOff) {
        return UNSAFE.getBoolean(obj, fieldOff);
    }

    /**
     * Stores boolean value into object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @param val Value.
     */
    public static void putBooleanField(Object obj, long fieldOff, boolean val) {
        UNSAFE.putBoolean(obj, fieldOff, val);
    }

    /**
     * Gets byte value from object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @return Byte value from object field.
     */
    public static byte getByteField(Object obj, long fieldOff) {
        return UNSAFE.getByte(obj, fieldOff);
    }

    /**
     * Stores byte value into object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @param val Value.
     */
    public static void putByteField(Object obj, long fieldOff, byte val) {
        UNSAFE.putByte(obj, fieldOff, val);
    }

    /**
     * Gets short value from object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @return Short value from object field.
     */
    public static short getShortField(Object obj, long fieldOff) {
        return UNSAFE.getShort(obj, fieldOff);
    }

    /**
     * Stores short value into object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @param val Value.
     */
    public static void putShortField(Object obj, long fieldOff, short val) {
        UNSAFE.putShort(obj, fieldOff, val);
    }

    /**
     * Gets char value from object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @return Char value from object field.
     */
    public static char getCharField(Object obj, long fieldOff) {
        return UNSAFE.getChar(obj, fieldOff);
    }

    /**
     * Stores char value into object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @param val Value.
     */
    public static void putCharField(Object obj, long fieldOff, char val) {
        UNSAFE.putChar(obj, fieldOff, val);
    }

    /**
     * Gets integer value from object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @return Integer value from object field.
     */
    public static int getIntField(Object obj, long fieldOff) {
        return UNSAFE.getInt(obj, fieldOff);
    }

    /**
     * Stores integer value into object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @param val Value.
     */
    public static void putIntField(Object obj, long fieldOff, int val) {
        UNSAFE.putInt(obj, fieldOff, val);
    }

    /**
     * Gets long value from object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @return Long value from object field.
     */
    public static long getLongField(Object obj, long fieldOff) {
        return UNSAFE.getLong(obj, fieldOff);
    }

    /**
     * Stores long value into object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @param val Value.
     */
    public static void putLongField(Object obj, long fieldOff, long val) {
        UNSAFE.putLong(obj, fieldOff, val);
    }

    /**
     * Gets float value from object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @return Float value from object field.
     */
    public static float getFloatField(Object obj, long fieldOff) {
        return UNSAFE.getFloat(obj, fieldOff);
    }

    /**
     * Stores float value into object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @param val Value.
     */
    public static void putFloatField(Object obj, long fieldOff, float val) {
        UNSAFE.putFloat(obj, fieldOff, val);
    }

    /**
     * Gets double value from object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @return Double value from object field.
     */
    public static double getDoubleField(Object obj, long fieldOff) {
        return UNSAFE.getDouble(obj, fieldOff);
    }

    /**
     * Stores double value into object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @param val Value.
     */
    public static void putDoubleField(Object obj, long fieldOff, double val) {
        UNSAFE.putDouble(obj, fieldOff, val);
    }

    /**
     * Gets reference from object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @return Reference from object field.
     */
    public static Object getObjectField(Object obj, long fieldOff) {
        return UNSAFE.getObject(obj, fieldOff);
    }

    /**
     * Stores reference value into object field.
     *
     * @param obj Object.
     * @param fieldOff Field offset.
     * @param val Value.
     */
    public static void putObjectField(Object obj, long fieldOff, Object val) {
        UNSAFE.putObject(obj, fieldOff, val);
    }

    /**
     * Stub.
     */
    private IgniteUnsafeUtils() {
    }
}
