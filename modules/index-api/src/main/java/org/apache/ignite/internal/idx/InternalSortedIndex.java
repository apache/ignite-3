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

package org.apache.ignite.internal.idx;

import java.util.BitSet;
import java.util.UUID;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.table.Tuple;

/**
 * Sorted index facade.
 */
public interface InternalSortedIndex {
    /** Exclude lower bound. */
    byte GREATER = 0;

    /** Include lower bound. */
    byte GREATER_OR_EQUAL = 1;

    /** Exclude upper bound. */
    byte LESS = 0;

    /** Include upper bound. */
    byte LESS_OR_EQUAL = 1 << 1;

    /**
     * Return index name.
     *
     * @return Index name.
     */
    String name();

    /**
     * Return index id.
     *
     * @return Index id.
     */
    UUID id();

    /**
     * Return indexed table.
     *
     * @return Indexed table identifier.
     */
    UUID tableId();

    /**
     * Return index descriptor.
     */
    SortedIndexDescriptor descriptor();

    /**
     * Return rows between lower and upper bounds. Fill results rows by fields specified at the projection set.
     *
     * @param low           Lower bound of the scan.
     * @param up            Lower bound of the scan.
     * @param scanBoundMask Scan bound mask (specify how to work with rows equals to the bounds: include or exclude).
     */
    Cursor<Tuple> scan(Tuple low, Tuple up, byte scanBoundMask, BitSet proj);

    /**
     * Drop index.
     */
    void drop();
}
