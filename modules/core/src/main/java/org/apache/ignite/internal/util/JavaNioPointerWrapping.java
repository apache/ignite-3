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

import java.lang.invoke.MethodHandle;
import java.nio.ByteBuffer;

/**
 * Uses the JavaNioAccess object to wraps a pointer to unmanaged memory into a direct byte buffer.
 */
class JavaNioPointerWrapping implements PointerWrapping {
    /** Null object. */
    private static final Object NULL_OBJ = null;

    private final MethodHandle newDirectBufMh;
    private final Object javaNioAccessObj;

    JavaNioPointerWrapping(MethodHandle newDirectBufMh, Object javaNioAccessObj) {
        this.newDirectBufMh = newDirectBufMh;
        this.javaNioAccessObj = javaNioAccessObj;
    }

    @Override
    public ByteBuffer wrapPointer(long ptr, int len) {
        try {
            ByteBuffer buf = (ByteBuffer) newDirectBufMh.invokeExact(javaNioAccessObj, ptr, len, NULL_OBJ);

            assert buf.isDirect() : "ptr=" + ptr + ", len=" + len;

            buf.order(GridUnsafe.NATIVE_BYTE_ORDER);

            return buf;
        } catch (Throwable e) {
            throw new RuntimeException("JavaNioAccess#newDirectByteBuffer() method is unavailable."
                    + FeatureChecker.JAVA_VER_SPECIFIC_WARN, e);
        }
    }
}
