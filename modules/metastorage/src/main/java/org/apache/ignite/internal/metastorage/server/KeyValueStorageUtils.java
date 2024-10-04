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

/** Helper class with useful methods and constants for {@link KeyValueStorage} implementations. */
public class KeyValueStorageUtils {
    /** Special value indicating that there are no key revisions that need to be compacted. */
    public static final int NOTHING_TO_COMPACT_INDEX = -1;

    /**
     * Calculates the revision index in key revisions up to which compaction is needed or {@link #NOTHING_TO_COMPACT_INDEX} if nothing
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
                return NOTHING_TO_COMPACT_INDEX;
            }

            i = -(i + 2);
        }

        if (i == keyRevisions.length - 1 && !isTombstone.test(keyRevisions[i])) {
            i = i == 0 ? NOTHING_TO_COMPACT_INDEX : i - 1;
        }

        return i;
    }

    /**
     * Converts bytes to UTF-8 string.
     *
     * @param bytes Bytes.
     */
    public static String toUtf8String(byte[] bytes) {
        return new String(bytes, UTF_8);
    }

    /** Asserts that the compaction revision is less than the current storage revision. */
    public static void assertCompactionRevisionLessThanCurrent(long compactionRevision, long revision) {
        assert compactionRevision < revision : String.format(
                "Compaction revision should be less than the current: [compaction=%s, current=%s]",
                compactionRevision, revision
        );
    }

    /** Asserts that the compaction revision is less than or equal the current storage revision. */
    public static void assertRequestedRevisionLessThanOrEqualCurrent(long requestedRevision, long revision) {
        assert requestedRevision <= revision : String.format(
                "Requested revision should be less than or equal the current: [requested=%s, current=%s]",
                requestedRevision, revision
        );
    }
}
