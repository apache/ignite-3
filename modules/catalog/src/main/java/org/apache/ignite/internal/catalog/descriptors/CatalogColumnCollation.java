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

package org.apache.ignite.internal.catalog.descriptors;

/**
 * Enumeration of all supported collations.
 */
public enum CatalogColumnCollation {
    ASC_NULLS_FIRST(true, true),
    ASC_NULLS_LAST(true, false),
    DESC_NULLS_FIRST(false, true),
    DESC_NULLS_LAST(false, false);

    private static final int ASC_FLAG_BITMASK = 1;
    private static final int NULLS_FIRST_FLAG_BITMASK = 1 << 1;

    private final boolean asc;
    private final boolean nullsFirst;

    /**
     * Constructs the collation object.
     *
     * @param asc Direction of the sorting.
     * @param nullsFirst Place of the null values in sorted range.
     */
    CatalogColumnCollation(boolean asc, boolean nullsFirst) {
        this.asc = asc;
        this.nullsFirst = nullsFirst;
    }

    /**
     * Returns collation object for given directions.
     *
     * @param asc Whether the values should be sorted in ascending order.
     * @param nullsFirst Whether to put null values first.
     * @return A collation object.
     */
    public static CatalogColumnCollation get(boolean asc, boolean nullsFirst) {
        if (asc && nullsFirst) {
            return ASC_NULLS_FIRST;
        } else if (asc) {
            return ASC_NULLS_LAST;
        } else if (nullsFirst) {
            return DESC_NULLS_FIRST;
        } else {
            return DESC_NULLS_LAST;
        }
    }

    /** Returns whether the column sorted in ascending order. */
    public boolean asc() {
        return asc;
    }

    /** Returns whether null values should be in the very beginning of the range. */
    public boolean nullsFirst() {
        return nullsFirst;
    }

    /** Writes collation to byte. */
    static byte pack(CatalogColumnCollation collation) {
        return  (byte) ((collation.asc() ? ASC_FLAG_BITMASK : 0) | (collation.nullsFirst() ? NULLS_FIRST_FLAG_BITMASK : 0));
    }

    /** Reads collation from byte. */
    static CatalogColumnCollation unpack(byte collationBits) {
        boolean asc = (collationBits & ASC_FLAG_BITMASK) == ASC_FLAG_BITMASK;
        boolean nullsFirst = (collationBits & NULLS_FIRST_FLAG_BITMASK) == NULLS_FIRST_FLAG_BITMASK;

        return get(asc, nullsFirst);
    }
}
