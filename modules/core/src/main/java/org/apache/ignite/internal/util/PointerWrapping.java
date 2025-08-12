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

package org.apache.ignite.internal.util;

import static org.apache.ignite.internal.util.GridUnsafe.NATIVE_BYTE_ORDER;
import static org.apache.ignite.internal.util.GridUnsafe.UNSAFE;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * Wraps a pointer to unmanaged memory into a direct byte buffer.
 */
abstract class PointerWrapping {
    /** Null object. */
    private static final Object NULL_OBJ = null;

    /** Handle for {@code JavaNioAccess#newDirectByteBuffer(long addr, int cap, Object ob)}. */
    private static final MethodHandle DIRECT_BUF_MTD;

    /** Handle for {@code DirectByteBuffer#DirectByteBuffer(long addr, int cap)}. */
    private static final MethodHandle DIRECT_BUF_CTOR_INT;

    /** Handle for {@code DirectByteBuffer#DirectByteBuffer(long addr, long cap)}. */
    private static final MethodHandle DIRECT_BUF_CTOR_LONG;

    /** Instance of {@code JavaNioAccess} retrieved from {@code SharedSecrets#getJavaNioAccess()}. */
    private static final Object JAVA_NIO_ACCESS_OBJ;

    static {
        Object nioAccessObj = null;

        MethodHandle directBufMtd = null;
        MethodHandle directBufCtorWithIntLen = null;
        MethodHandle directBufCtorWithLongLen = null;

        try {
            directBufCtorWithIntLen = createAndTestNewDirectBufferCtor(int.class);
        } catch (Throwable e) {
            try {
                directBufCtorWithLongLen = createAndTestNewDirectBufferCtor(long.class);
            } catch (Throwable e1) {
                e.addSuppressed(e1);

                try {
                    nioAccessObj = javaNioAccessObject();
                    directBufMtd = newDirectBufferMethodHandle(nioAccessObj);
                } catch (Throwable e2) {
                    e.addSuppressed(e2);
                }

                if (nioAccessObj == null || directBufMtd == null) {
                    throw e;
                }
            }
        }

        DIRECT_BUF_MTD = directBufMtd;
        DIRECT_BUF_CTOR_INT = directBufCtorWithIntLen;
        DIRECT_BUF_CTOR_LONG = directBufCtorWithLongLen;

        JAVA_NIO_ACCESS_OBJ = nioAccessObj;
    }

    /**
     * Wraps a pointer to unmanaged memory into a direct byte buffer.
     *
     * @param ptr Pointer to wrap.
     * @param len Memory location length.
     * @return Byte buffer wrapping the given memory.
     */
    static ByteBuffer wrapPointer(long ptr, int len) {
        // Direct usage of "static final" method handle instances allows JIT to inline them into a compiled code. Such an approach produces
        // the most optimal results.
        // Usage of conditional branching here is not a problem, because they only refer to "static final" values too, which means that
        // eventually JIT will inline them and perform a dead branch elimination.
        // Please be aware of that if you want to refactor this code. Don't do it without having benchmarks and guaranteeing that
        // performance won't suffer.
        if (DIRECT_BUF_MTD != null && JAVA_NIO_ACCESS_OBJ != null) {
            return wrapPointerJavaNio(ptr, len);
        } else if (DIRECT_BUF_CTOR_INT != null) {
            return wrapPointerDirectBufferConstructor(ptr, len);
        } else if (DIRECT_BUF_CTOR_LONG != null) {
            return wrapPointerDirectBufferConstructor(ptr, (long) len);
        } else {
            throw new AssertionError(
                    "All alternatives for a new DirectByteBuffer() creation failed: " + FeatureChecker.JAVA_STARTUP_PARAMS_WARN);
        }
    }

    /**
     * Returns {@code JavaNioAccess} instance from private API for corresponding Java version.
     *
     * @return {@code JavaNioAccess} instance for corresponding Java version.
     * @throws AssertionError If getting access to the private API is failed.
     */
    private static Object javaNioAccessObject() {
        Class<?> cls;
        try {
            cls = Class.forName("jdk.internal.access.SharedSecrets");
        } catch (ClassNotFoundException e) {
            try {
                cls = Class.forName("jdk.internal.misc.SharedSecrets");
            } catch (ClassNotFoundException e1) {
                throw new AssertionError("Neither jdk.internal.access.SharedSecrets nor jdk.internal.misc.SharedSecrets are unavailable."
                        + FeatureChecker.JAVA_STARTUP_PARAMS_WARN, e);
            }
        }

        try {
            Method mth = cls.getMethod("getJavaNioAccess");

            return mth.invoke(null);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(cls.getName() + " class is unavailable."
                    + FeatureChecker.JAVA_STARTUP_PARAMS_WARN, e);
        }
    }

    /**
     * Returns reference to {@code JavaNioAccess.newDirectByteBuffer} method from private API for corresponding Java version.
     *
     * @param nioAccessObj Java NIO access object.
     * @return Reference to {@code JavaNioAccess.newDirectByteBuffer} method
     * @throws AssertionError If getting access to the private API is failed.
     */
    private static MethodHandle newDirectBufferMethodHandle(Object nioAccessObj) {
        try {
            Class<?> cls = nioAccessObj.getClass();

            Method mtd = cls.getMethod("newDirectByteBuffer", long.class, int.class, Object.class);

            AccessController.doPrivileged((PrivilegedExceptionAction<?>) () -> {
                mtd.setAccessible(true);

                return null;
            });

            MethodType mtdType = MethodType.methodType(
                    ByteBuffer.class,
                    Object.class,
                    long.class,
                    int.class,
                    Object.class
            );

            return MethodHandles.lookup()
                    .unreflect(mtd)
                    .asType(mtdType);
        } catch (ReflectiveOperationException | PrivilegedActionException e) {
            throw new AssertionError(nioAccessObj.getClass().getName() + "#newDirectByteBuffer() method is unavailable."
                    + FeatureChecker.JAVA_STARTUP_PARAMS_WARN, e);
        }
    }

    /**
     * Creates and tests constructor for Direct ByteBuffer. Test is wrapping one-byte unsafe memory into a buffer.
     *
     * @param lengthType Type of the length parameter.
     * @return constructor for creating direct ByteBuffers.
     */
    private static MethodHandle createAndTestNewDirectBufferCtor(Class<?> lengthType) {
        assert lengthType == int.class || lengthType == long.class : "Unsupported type of length: " + lengthType;

        MethodHandle ctorCandidate = createNewDirectBufferCtor(lengthType);

        int l = 1;
        long ptr = UNSAFE.allocateMemory(l);

        try {
            ByteBuffer buf = lengthType == int.class
                    ? (ByteBuffer) ctorCandidate.invokeExact(ptr, l)
                    : (ByteBuffer) ctorCandidate.invokeExact(ptr, (long) l);

            if (!buf.isDirect()) {
                throw new IllegalArgumentException("Buffer expected to be direct, internal error during #wrapPointerDirectBufCtor()");
            }
        } catch (Throwable t) {
            throw new AssertionError(t);
        } finally {
            UNSAFE.freeMemory(ptr);
        }

        return ctorCandidate;
    }

    /**
     * Simply create some instance of direct Byte Buffer and try to get it's class declared constructor.
     *
     * @param lengthType Type of the length parameter.
     * @return constructor for creating direct ByteBuffers.
     */
    private static MethodHandle createNewDirectBufferCtor(Class<?> lengthType) {
        try {
            ByteBuffer buf = ByteBuffer.allocateDirect(1).order(NATIVE_BYTE_ORDER);

            Constructor<?> ctor = buf.getClass().getDeclaredConstructor(long.class, lengthType);

            AccessController.doPrivileged((PrivilegedExceptionAction<?>) () -> {
                ctor.setAccessible(true);

                return null;
            });

            MethodType mtdType = MethodType.methodType(
                    ByteBuffer.class,
                    long.class,
                    lengthType
            );

            return MethodHandles.lookup()
                    .unreflectConstructor(ctor)
                    .asType(mtdType);
        } catch (ReflectiveOperationException | PrivilegedActionException e) {
            throw new AssertionError("Unable to set up byte buffer creation using reflection :" + e.getMessage(), e);
        }
    }

    private static ByteBuffer wrapPointerJavaNio(long ptr, int len) {
        try {
            ByteBuffer buf = (ByteBuffer) DIRECT_BUF_MTD.invokeExact(JAVA_NIO_ACCESS_OBJ, ptr, len, NULL_OBJ);

            assert buf.isDirect() : "ptr=" + ptr + ", len=" + len;

            buf.order(NATIVE_BYTE_ORDER);

            return buf;
        } catch (Throwable e) {
            throw new AssertionError("JavaNioAccess#newDirectByteBuffer() method is unavailable."
                    + FeatureChecker.JAVA_STARTUP_PARAMS_WARN, e);
        }
    }

    /**
     * Wraps a pointer to unmanaged memory into a direct byte buffer. Uses the constructor of the direct byte buffer.
     *
     * @param ptr Pointer to wrap.
     * @param len Memory location length.
     * @return Byte buffer wrapping the given memory.
     */
    private static ByteBuffer wrapPointerDirectBufferConstructor(long ptr, int len) {
        try {
            ByteBuffer newDirectBuf = (ByteBuffer) DIRECT_BUF_CTOR_INT.invokeExact(ptr, len);

            return newDirectBuf.order(NATIVE_BYTE_ORDER);
        } catch (Throwable e) {
            throw new AssertionError("DirectByteBuffer#constructor is unavailable."
                    + FeatureChecker.JAVA_STARTUP_PARAMS_WARN, e);
        }
    }

    /**
     * Wraps a pointer to unmanaged memory into a direct byte buffer. Uses the constructor of the direct byte buffer.
     *
     * @param ptr Pointer to wrap.
     * @param len Memory location length.
     * @return Byte buffer wrapping the given memory.
     */
    private static ByteBuffer wrapPointerDirectBufferConstructor(long ptr, long len) {
        try {
            ByteBuffer newDirectBuf = (ByteBuffer) DIRECT_BUF_CTOR_LONG.invokeExact(ptr, len);

            return newDirectBuf.order(NATIVE_BYTE_ORDER);
        } catch (Throwable e) {
            throw new AssertionError("DirectByteBuffer#constructor is unavailable."
                    + FeatureChecker.JAVA_STARTUP_PARAMS_WARN, e);
        }
    }
}
