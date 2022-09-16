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

package org.apache.ignite.internal.pagememory.persistence;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.function.BiConsumer;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.jetbrains.annotations.Nullable;

/**
 * Interface for storing correspondence of page ID in a group to long value (address in offheap segment).
 *
 * <p>Map support versioning of
 * entries. Outdated entry (entry having version lower than requested), is not provided in case of get, outdated return value is provided
 * instead.
 *
 * <p>This mapping is not thread safe. Operations should be protected by outside locking.<br>
 */
public interface LoadedPagesMap {
    /**
     * Gets value associated with the given key.
     *
     * @param grpId Group ID. First part of the key.
     * @param pageId Page ID. Second part of the key.
     * @param reqVer Requested entry version, counter associated with value.
     * @param absent Return if provided page is not presented in map.
     * @param outdated Return if provided {@code reqVer} version is greater than value in map (was used for put).
     * @return A value associated with the given key.
     */
    long get(int grpId, long pageId, int reqVer, long absent, long outdated);

    /**
     * Associates the given key with the given value.
     *
     * @param grpId Group ID. First part of the key.
     * @param pageId Page ID. Second part of the key.
     * @param val Value to set.
     * @param ver Version/counter associated with value, can be used to check if value is outdated.
     */
    void put(int grpId, long pageId, long val, int ver);

    /**
     * Refresh outdated value. Sets provided version to value associated with group and page. Method should be called only for key present
     * and only if version was outdated. Method may be called in case {@link #get(int, long, int, long, long)} returned {@code outdated}
     * return value.
     *
     * @param grpId First part of the key. Group ID.
     * @param pageId Second part of the key. Page ID.
     * @param ver Partition tag.
     * @return A value associated with the given key.
     * @throws IllegalArgumentException if method is called for absent key or key with fresh version.
     */
    long refresh(int grpId, long pageId, int ver);

    /**
     * Removes key-value association for the given key.
     *
     * @param grpId First part of the key. Group ID.
     * @param pageId Second part of the key. Page ID.
     * @return {@code True} if value was actually found and removed.
     */
    boolean remove(int grpId, long pageId);

    /**
     * Returns maximum number of entries in the map. This maximum can not be always reached.
     */
    int capacity();

    /**
     * Returns current number of entries in the map.
     */
    int size();

    /**
     * Find the nearest presented value from specified position to the right.
     *
     * @param idxStart Index to start searching from. Bounded with {@link #capacity()}.
     * @return Closest value to the index, and it's partition tag or {@code null} value that will be returned if no values present.
     */
    @Nullable ReplaceCandidate getNearestAt(int idxStart);

    /**
     * Removes entities matching provided predicate at specified mapping range.
     *
     * @param startIdxToClear Index to clear value at, inclusive. Bounded with {@link #capacity()}.
     * @param endIdxToClear Index to clear value at, inclusive. Bounded with {@link #capacity()}.
     * @param keyPred Test predicate for (group ID, page ID).
     * @return List with removed values, value is not added to list for empty cell or if key is not matching to predicate.
     */
    LongArrayList removeIf(int startIdxToClear, int endIdxToClear, KeyPredicate keyPred);

    /**
     * Removes entities matching provided predicate.
     *
     * @param keyPred Test predicate for (group ID, page ID).
     * @return List with removed values, value is not added to list for empty cell or if key is not matching to predicate.
     */
    default LongArrayList removeIf(KeyPredicate keyPred) {
        return removeIf(0, capacity(), keyPred);
    }

    /**
     * Scans all the elements in this table.
     *
     * @param act Visitor/action to be applied to each not empty cell.
     */
    void forEach(BiConsumer<FullPageId, Long> act);

    /**
     * Interface describing a predicate for Key (group ID, page ID). Usage of this predicate prevents odd object creation.
     */
    @FunctionalInterface
    interface KeyPredicate {
        /**
         * Predicate body.
         *
         * @param grpId Group ID.
         * @param pageId Page ID.
         */
        boolean test(int grpId, long pageId);
    }
}
