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

package org.apache.ignite.internal.schema;

/**
 * A thin wrapper over {@link NativeTypeSpec} to instantiate parameterized constrained types.
 */
public class NativeTypes {
    /** */
    public static final NativeType BYTE = new NativeType(NativeTypeSpec.BYTE, 1);

    /** */
    public static final NativeType SHORT = new NativeType(NativeTypeSpec.SHORT, 2);

    /** */
    public static final NativeType INTEGER = new NativeType(NativeTypeSpec.INTEGER, 4);

    /** */
    public static final NativeType LONG = new NativeType(NativeTypeSpec.LONG, 8);

    /** */
    public static final NativeType FLOAT = new NativeType(NativeTypeSpec.FLOAT, 4);

    /** */
    public static final NativeType DOUBLE = new NativeType(NativeTypeSpec.DOUBLE, 8);

    /** */
    public static final NativeType UUID = new NativeType(NativeTypeSpec.UUID, 16);

    /** */
    public static final NativeType STRING = new VarlenNativeType(NativeTypeSpec.STRING, Integer.MAX_VALUE);

    /** */
    public static final NativeType BYTES = new VarlenNativeType(NativeTypeSpec.BYTES, Integer.MAX_VALUE);

    /** Don't allow to create an instance. */
    private NativeTypes() {
    }

    /**
     * Creates a bitmask type of size <code>bits</code>. In row will round up to the closest full byte.
     *
     * @param bits The number of bits in the bitmask.
     */
    public static NativeType bitmaskOf(int bits) {
        return BitmaskNativeType.of(bits);
    }

    /**
     * Creates a STRING type with maximal length is <code>len</code>.
     *
     * @param len Maximum length of the string.
     */
    public static NativeType stringOf(int len) {
        return new VarlenNativeType(NativeTypeSpec.STRING, len);
    }

    /**
     * Creates a BYTES type with maximal length is <code>len</code>.
     *
     * @param len Maximum length of the byte array.
     */
    public static NativeType blobOf(int len) {
        return new VarlenNativeType(NativeTypeSpec.BYTES, len);
    }
}
