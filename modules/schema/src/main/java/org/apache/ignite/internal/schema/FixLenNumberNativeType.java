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

import org.apache.ignite.internal.tostring.S;

/**
 * A fixed-sized type representing a BigInteger of <code>n</code> bytes.
 */
public class FixLenNumberNativeType extends NativeType {
    /** */
    private final int precision;

    /**
     * Creates a number type of size <code>bytes</code>.
     *
     * @param precision The maximum number of bytes in the BigInteger.
     */
    protected FixLenNumberNativeType(int precision) {
        super(NativeTypeSpec.NUMBER, NumericTypeUtils.byteSizeByPrecision(precision));

        this.precision = precision;
    }

    /**
     * @return Maximum number of bytes to be stored in the BigInteger.
     */
    public int precision() {
        return precision;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        FixLenNumberNativeType that = (FixLenNumberNativeType)o;

        return precision == that.precision;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return precision;
    }

    /** {@inheritDoc} */
    @Override public int compareTo(NativeType o) {
        int res = super.compareTo(o);

        if (res == 0) {
            // The passed in object is also a number, compare the number of bytes.
            FixLenNumberNativeType that = (FixLenNumberNativeType)o;

            return Integer.compare(precision, that.precision);
        }
        else
            return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(FixLenNumberNativeType.class.getSimpleName(), "typeSpec", spec(), "len", sizeInBytes());
    }
}
