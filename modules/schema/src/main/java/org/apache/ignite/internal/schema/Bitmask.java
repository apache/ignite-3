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
 * A fixed-sized type representing a bitmask of <code>n</code> bits. The actual size of a bitmask will round up
 * to the smallest number of bytes required to store <code>n</code> bits.
 */
public class Bitmask extends NativeType {
    /** */
    private final int bits;

    /**
     * Factory method for creating the bitmask type.
     *
     * @param nBits Maximum number of bits in the bitmask.
     * @return Bitmask type.
     */
    public static Bitmask of(int nBits) {
        return new Bitmask(nBits);
    }

    /**
     * Creates a bitmask type of size <code>bits</code>. In tuple will round up to the closest full byte.
     *
     * @param bits The number of bits in the bitmask.
     */
    protected Bitmask(int bits) {
        super(NativeTypeSpec.BITMASK, (bits + 7) / 8);

        this.bits = bits;
    }

    /**
     * @return Maximum number of bits to be stored in the bitmask.
     */
    public int bits() {
        return bits;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        Bitmask that = (Bitmask)o;

        return bits == that.bits;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return bits;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(NativeType o) {
        int res = super.compareTo(o);

        if (res == 0) {
            // The passed in object is also a bitmask, compare the number of bits.
            Bitmask that = (Bitmask)o;

            return Integer.compare(bits, that.bits);
        }
        else
            return res;
    }
}
