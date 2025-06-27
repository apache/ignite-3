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

import java.util.function.LongPredicate;
import org.jetbrains.annotations.Nullable;

/** Helper class with useful methods and constants for {@link KeyValueStorage} implementations. */
public class KeyValueStorageUtils {
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
     * @param compactionRevision Revision up to which you need to compact (inclusive).
     * @param isTombstone Predicate to test whether a key revision is a tombstone.
     */
    public static int indexToCompact(long[] keyRevisions, long compactionRevision, LongPredicate isTombstone) {
        int i = binarySearch(keyRevisions, compactionRevision);

        if (i < 0) {
            i = ~i;

            if (i == 0) {
                return NOT_FOUND;
            }

            i--;
        }

        // If revision for "i" points to a tombstone, it can be safely deleted immediately.
        if (isTombstone.test(keyRevisions[i])) {
            return i;
        }

        // Revision for "i" does not point to a tombstone. Next revision for the key is "compactionRevision + 1", which means that there are
        // no revisions strictly between "next" and revision for "i". In that case "keyRevision[i]" can be deleted, because it can't
        // possibly be read for any read with a revision above "compactionRevision".
        if (i != keyRevisions.length - 1 && keyRevisions[i + 1] == compactionRevision + 1) {
            return i;
        }

        // In this case, revision "keyRevision[i]" can be read from a point of view of revision "keyRevision[i] + 1", and thus we must only
        // delete a revision previous to "keyRevision[i]".
        return i == 0 ? NOT_FOUND : i - 1;
    }

    /**
     * Returns index of maximum revision which is less or equal to {@code upperBoundRevision}. If there is no such revision then
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

    /**
     * Returns index of minimum revision which is greater or equal to {@code lowerBoundRevision}. If there is no such revision then
     * {@link #NOT_FOUND} will be returned.
     *
     * @param keyRevisions Metastorage key revisions in ascending order.
     * @param lowerBoundRevision Revision lower bound.
     */
    public static int minRevisionIndex(long[] keyRevisions, long lowerBoundRevision) {
        int i = binarySearch(keyRevisions, lowerBoundRevision);

        if (i < 0) {
            if (i == -(keyRevisions.length + 1)) {
                return NOT_FOUND;
            }

            i = -(i + 1);
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
}
