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

package org.apache.ignite.internal.index;

import java.util.BitSet;
import org.apache.ignite.internal.schema.BinaryTuple;
import org.apache.ignite.internal.util.Cursor;

/**
 * An object describing a sorted index.
 *
 * <p>Extends a basic index with operations related to sorted structures.
 */
public interface SortedIndex extends Index<SortedIndexDescriptor> {
    /** A flag denotes that left bound should be included into result. */
    byte INCLUDE_LEFT = 0b01;

    /** A flag denotes that right bound should be included into result. */
    byte INCLUDE_RIGHT = 0b10;

    /**
     * Opens a range cursor for given bounds with left bound included in result and right excluded.
     *
     * @param left Left bound of range.
     * @param right Right bound of range.
     * @param columns Columns to include.
     * @return A cursor from resulting rows.
     */
    default Cursor<BinaryTuple> scan(BinaryTuple left, BinaryTuple right, BitSet columns) {
        return scan(left, right, INCLUDE_LEFT, columns);
    }

    /**
     * Opens a range cursor for given bounds. Inclusion of the bounds is defined by {@code includeBounds} mask.
     *
     * @param left Left bound of range.
     * @param right Right bound of range.
     * @param includeBounds A mask that defines whether to include bounds into the final result or not.
     * @param columns Columns to include.
     * @return A cursor from resulting rows.
     * @see SortedIndex#INCLUDE_LEFT
     * @see SortedIndex#INCLUDE_RIGHT
     */
    Cursor<BinaryTuple> scan(BinaryTuple left, BinaryTuple right, byte includeBounds, BitSet columns);
}
