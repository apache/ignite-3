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
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.NOTHING_TO_COMPACT_INDEX;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.indexToCompact;
import static org.apache.ignite.internal.metastorage.server.KeyValueStorageUtils.toUtf8String;
import static org.apache.ignite.internal.util.ArrayUtils.LONG_EMPTY_ARRAY;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/** For {@link KeyValueStorageUtils} testing. */
public class KeyValueStorageUtilsTest {
    @Test
    void testIndexToCompactNoRevisions() {
        assertEquals(NOTHING_TO_COMPACT_INDEX, indexToCompact(LONG_EMPTY_ARRAY, 0, revision -> false));
        assertEquals(NOTHING_TO_COMPACT_INDEX, indexToCompact(LONG_EMPTY_ARRAY, 0, revision -> true));
    }

    @Test
    void testIndexToCompactSingleRevision() {
        long[] keyRevisions = {2};

        assertEquals(NOTHING_TO_COMPACT_INDEX, indexToCompact(keyRevisions, 1, revision -> false));
        assertEquals(NOTHING_TO_COMPACT_INDEX, indexToCompact(keyRevisions, 1, revision -> true));

        assertEquals(0, indexToCompact(keyRevisions, 2, revision -> true));
        assertEquals(NOTHING_TO_COMPACT_INDEX, indexToCompact(keyRevisions, 2, revision -> false));

        assertEquals(0, indexToCompact(keyRevisions, 3, revision -> true));
        assertEquals(NOTHING_TO_COMPACT_INDEX, indexToCompact(keyRevisions, 3, revision -> false));
    }

    @Test
    void testIndexToCompactMultipleRevision() {
        long[] keyRevisions = {2, 4, 5};

        assertEquals(NOTHING_TO_COMPACT_INDEX, indexToCompact(keyRevisions, 1, revision -> true));
        assertEquals(NOTHING_TO_COMPACT_INDEX, indexToCompact(keyRevisions, 1, revision -> false));

        assertEquals(0, indexToCompact(keyRevisions, 2, revision -> true));
        assertEquals(0, indexToCompact(keyRevisions, 2, revision -> false));

        assertEquals(0, indexToCompact(keyRevisions, 3, revision -> true));
        assertEquals(0, indexToCompact(keyRevisions, 3, revision -> false));

        assertEquals(1, indexToCompact(keyRevisions, 4, revision -> true));
        assertEquals(1, indexToCompact(keyRevisions, 4, revision -> false));

        assertEquals(2, indexToCompact(keyRevisions, 5, revision -> true));
        assertEquals(1, indexToCompact(keyRevisions, 5, revision -> false));

        assertEquals(2, indexToCompact(keyRevisions, 6, revision -> true));
        assertEquals(1, indexToCompact(keyRevisions, 6, revision -> false));
    }

    @Test
    void testToUtf8String() {
        assertEquals("foo", toUtf8String("foo".getBytes(UTF_8)));
    }
}
