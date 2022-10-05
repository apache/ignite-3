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

package org.apache.ignite.internal.storage.pagememory.index;

import static org.apache.ignite.internal.binarytuple.BinaryTupleCommon.HEADER_SIZE;
import static org.apache.ignite.internal.binarytuple.BinaryTupleCommon.nullMapSize;
import static org.apache.ignite.internal.util.Constants.KiB;

import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.schema.BinaryTupleSchema;

/**
 * Calculator of inline size in bytes.
 */
public class InlineSizeCalculator {
    /** Heuristic maximum inline size in bytes for storing entries in B+tree InnerNodes. */
    public static final int MAX_INLINE_SIZE = 2 * KiB;

    /** Heuristic inline size in bytes for variable length columns, such as strings. */
    public static final int VARLEN_COLUMN_INLINE_SIZE = 10;

    /** Heuristic size class in bytes for offset table in {@link BinaryTuple}. */
    public static final int TABLE_OFFSET_CLASS_SIZE = 2;

    /**
     * Calculates the inline size for a {@link BinaryTuple}.
     *
     * <p>Binary tuple format is taken into account:
     * <ul>
     *     <li>Header - 1 byte;</li>
     *     <li>Nullmap (if there are null columns) - (Number of columns + 7) / 8 bytes;</li>
     *     <li>Offset table - (Number of columns * {@link #TABLE_OFFSET_CLASS_SIZE}) bytes;</li>
     *     <li>Value area - total size of all column types ({@link #VARLEN_COLUMN_INLINE_SIZE} for varlen columns) in bytes.</li>
     * </ul>
     *
     * @param schema Binary tuple schema.
     * @return Inline size in bytes.
     */
    public static int calculateInlineSize(BinaryTupleSchema schema) {
        int inlineSize = HEADER_SIZE
                + (schema.hasNullableElements() ? nullMapSize(schema.elementCount()) : 0)
                + schema.elementCount() * TABLE_OFFSET_CLASS_SIZE;

        // TODO: IGNITE-17536 надо будет переделать немного

        return Math.min(MAX_INLINE_SIZE, inlineSize);
    }
}
