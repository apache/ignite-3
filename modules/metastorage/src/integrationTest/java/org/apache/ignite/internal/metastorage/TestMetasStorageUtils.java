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

package org.apache.ignite.internal.metastorage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Objects;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.jetbrains.annotations.Nullable;

/** Helper class for use in integration tests that may contain useful methods and constants. */
public class TestMetasStorageUtils {
    /** Special value representing any random timestamp. */
    public static final HybridTimestamp ANY_TIMESTAMP = new HybridTimestamp(1L, 0);

    /** Checks the metastore entry. */
    public static void checkEntry(Entry actEntry, byte[] expKey, byte @Nullable [] expValue, long expRevision, long expUpdateCounter) {
        assertEquals(expRevision, actEntry.revision(), () -> "entry=" + actEntry);
        assertEquals(expUpdateCounter, actEntry.updateCounter(), () -> "entry=" + actEntry);
        assertArrayEquals(expKey, actEntry.key(), () -> "entry=" + actEntry);
        assertArrayEquals(expValue, actEntry.value(), () -> "entry=" + actEntry);
    }

    /** Returns {@code true} entries are equal. */
    public static boolean equals(Entry act, Entry exp) {
        if (act.revision() != exp.revision()) {
            return false;
        }

        if (act.updateCounter() != exp.updateCounter()) {
            return false;
        }

        if (act.timestamp() != ANY_TIMESTAMP && exp.timestamp() != ANY_TIMESTAMP) {
            if (!Objects.equals(act.timestamp(), exp.timestamp())) {
                return false;
            }
        }

        if (!Arrays.equals(act.key(), exp.key())) {
            return false;
        }

        return Arrays.equals(act.value(), exp.value());
    }
}
