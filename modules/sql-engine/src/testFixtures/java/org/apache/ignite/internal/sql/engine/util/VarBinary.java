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

package org.apache.ignite.internal.sql.engine.util;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.ignite.internal.type.NativeTypes;

/**
 * A wrapper for {@link NativeTypes#BYTES} that implements {@link Comparable} interface.
 */
public final class VarBinary implements NativeTypeWrapper<VarBinary> {

    private final byte[] bytes;

    /** Constructor. */
    private VarBinary(byte[] bytes) {
        this.bytes = bytes;
    }

    /** Creates a var binary object from the given bytes array. */
    public static VarBinary fromBytes(byte[] bytes) {
        return new VarBinary(bytes);
    }

    /** Creates a varbinary object from UTF-8 bytes of the given string. */
    public static VarBinary fromUtf8String(String string) {
        return new VarBinary(string.getBytes(StandardCharsets.UTF_8));
    }

    /** Creates a var binary object from the given bytes array. */
    public static VarBinary varBinary(byte[] bytes) {
        return new VarBinary(bytes);
    }

    /** Converts this var binary object ot a java string of the given {@link Charset}. */
    public String asString(Charset charset) {
        // Renamed to asString because toString(Charset) triggers an error in checkstyle,
        // which can not distinguish between toString() and toString(Charset).
        return new String(bytes, charset);
    }

    /** {@inheritDoc} */
    @Override
    public byte[] get() {
        return bytes;
    }

    /** {@inheritDoc} */
    @Override
    public int compareTo(VarBinary o) {
        return Arrays.compare(bytes, o.bytes);
    }

    /** {@inheritDoc} */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VarBinary varBinary = (VarBinary) o;
        return Arrays.equals(bytes, varBinary.bytes);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return asString(StandardCharsets.UTF_8);
    }
}
