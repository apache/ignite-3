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

package org.apache.ignite.internal.metastorage.server;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.binarySearch;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.TreeSet;
import java.util.function.LongPredicate;
import java.util.function.Predicate;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.jetbrains.annotations.Nullable;

/** Helper class with useful methods and constants for {@link KeyValueStorage} implementations. */
public class KeyValueStorageUtils {
    /** Lexicographic order comparator. */
    public static final Comparator<byte[]> KEY_BYTES_COMPARATOR = Arrays::compareUnsigned;

    /** Constant meaning something could not be found. */
    public static final int NOT_FOUND = -1;

    /**
     * Calculates the revision index in key revisions up to which compaction is needed or {@link #NOT_FOUND} if nothing
     * needs to be compacted.
     *
     * <p>If the returned index points to the last revision and if the last revision is <b>not</b> a tombstone, then the returned index is
     * decremented by 1.</p>
     *
     * @param keyRevisions Metastorage key revisions in ascending order.
     * @param compactionRevisionInclusive Revision up to which you need to compact (inclusive).
     * @param isTombstone Predicate to test whether a key revision is a tombstone.
     */
    public static int indexToCompact(long[] keyRevisions, long compactionRevisionInclusive, LongPredicate isTombstone) {
        int i = binarySearch(keyRevisions, compactionRevisionInclusive);

        if (i < 0) {
            if (i == -1) {
                return NOT_FOUND;
            }

            i = -(i + 2);
        }

        if (i == keyRevisions.length - 1 && !isTombstone.test(keyRevisions[i])) {
            i = i == 0 ? NOT_FOUND : i - 1;
        }

        return i;
    }

    /**
     * Returns index of maximum revision which must be less or equal to {@code upperBoundRevision}. If there is no such revision then
     * {@link #NOT_FOUND} will be returned.
     *
     * @param keyRevisions Metastorage key revisions in ascending order.
     * @param upperBoundRevision Revision upper bound.
     */
    public static int maxRevisionIndex(long[] keyRevisions, long upperBoundRevision) {
        int i = binarySearch(keyRevisions, upperBoundRevision);

        if (i < 0) {
            if (i == -1) {
                return NOT_FOUND;
            }

            i = -(i + 2);
        }

        return i;
    }

    /** Returns {@link true} if the requested index is the last index of the array. */
    public static boolean isLastIndex(long[] arr, int index) {
        assert index >= 0 && index < arr.length : "index=" + index + ", arr.length=" + arr.length;

        return arr.length - 1 == index;
    }

    /**
     * Converts bytes to UTF-8 string.
     *
     * @param bytes Bytes.
     */
    public static String toUtf8String(byte @Nullable [] bytes) {
        return bytes == null ? "null" : new String(bytes, UTF_8);
    }

    /** Asserts that the compaction revision is less than the current storage revision. */
    public static void assertCompactionRevisionLessThanCurrent(long compactionRevision, long revision) {
        assert compactionRevision < revision : String.format(
                "Compaction revision should be less than the current: [compaction=%s, current=%s]",
                compactionRevision, revision
        );
    }

    /** Asserts that the requested revision is less than or equal to the current storage revision. */
    public static void assertRequestedRevisionLessThanOrEqualToCurrent(long requestedRevision, long revision) {
        assert requestedRevision <= revision : String.format(
                "Requested revision should be less than or equal to the current: [requested=%s, current=%s]",
                requestedRevision, revision
        );
    }

    /** Creates a predicate for {@link KeyValueStorage#watchRange(byte[], byte[], long, WatchListener)}. */
    public static Predicate<byte[]> watchRangeKeyPredicate(byte[] keyFrom, byte @Nullable [] keyTo) {
        return keyTo == null
                ? k -> KEY_BYTES_COMPARATOR.compare(keyFrom, k) <= 0
                : k -> KEY_BYTES_COMPARATOR.compare(keyFrom, k) <= 0 && KEY_BYTES_COMPARATOR.compare(keyTo, k) > 0;
    }

    /** Creates a predicate for {@link KeyValueStorage#watchExact(byte[], long, WatchListener)}. */
    public static Predicate<byte[]> watchExactKeyPredicate(byte[] key) {
        return k -> KEY_BYTES_COMPARATOR.compare(k, key) == 0;
    }

    /** Creates a predicate for {@link KeyValueStorage#watchExact(Collection, long, WatchListener)}. */
    public static Predicate<byte[]> watchExactKeyPredicate(Collection<byte[]> keys) {
        var keySet = new TreeSet<>(KEY_BYTES_COMPARATOR);

        keySet.addAll(keys);

        return keySet::contains;
    }
}
