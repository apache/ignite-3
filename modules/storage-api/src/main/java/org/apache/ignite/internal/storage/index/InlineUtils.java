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

package org.apache.ignite.internal.storage.index;

import java.math.BigDecimal;
import java.math.BigInteger;
import org.apache.ignite.internal.schema.NativeType;
import org.apache.ignite.internal.schema.NativeTypeSpec;
import org.apache.ignite.internal.schema.VarlenNativeType;

/**
 * Helper class for index inlining.
 */
public class InlineUtils {
    /** Inline size of an undefined (not set by the user) variable length column in bytes. */
    public static final int UNDEFINED_VARLEN_INLINE_SIZE = 10;

    /** Inline size for large numbers ({@link BigDecimal} and {@link BigInteger}) in bytes. */
    public static final int BIG_NUMBER_INLINE_SIZE = 4;

    /**
     * Calculate the size of the inline for the column.
     *
     * @param nativeType Column type.
     * @return Inline size in bytes.
     */
    static int inlineSize(NativeType nativeType) {
        NativeTypeSpec spec = nativeType.spec();

        if (spec.fixedLength()) {
            return nativeType.sizeInBytes();
        }

        // Variable length columns.

        switch (spec) {
            case STRING: {
                int length = ((VarlenNativeType) nativeType).length();

                return length == Integer.MAX_VALUE ? 10 : length * 2;
            }

            case BYTES: {
                int length = ((VarlenNativeType) nativeType).length();

                return length == Integer.MAX_VALUE ? 10 : length;
            }

            case DECIMAL:
            case NUMBER:
                return BIG_NUMBER_INLINE_SIZE;

            default:
                throw new IllegalArgumentException("Unknown type " + spec);
        }
    }
}
