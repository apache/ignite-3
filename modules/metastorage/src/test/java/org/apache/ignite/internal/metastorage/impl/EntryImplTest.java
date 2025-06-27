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

package org.apache.ignite.internal.metastorage.impl;

import static org.apache.ignite.internal.hlc.HybridTimestamp.hybridTimestamp;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ignite.internal.metastorage.Entry;
import org.junit.jupiter.api.Test;

/** For {@link EntryImpl} testing. */
public class EntryImplTest {
    private static final byte[] KEY = {0};

    @Test
    void testEmpty() {
        Entry entry = EntryImpl.empty(KEY);

        assertTrue(entry.empty());
        assertFalse(entry.tombstone());

        assertEquals(0, entry.revision());

        assertArrayEquals(KEY, entry.key());

        assertNull(entry.value());
        assertNull(entry.timestamp());
    }

    @Test
    void testTombstone() {
        Entry entry = EntryImpl.tombstone(KEY, 1, hybridTimestamp(10L));

        assertFalse(entry.empty());
        assertTrue(entry.tombstone());

        assertEquals(1, entry.revision());
        assertEquals(hybridTimestamp(10L), entry.timestamp());

        assertArrayEquals(KEY, entry.key());

        assertNull(entry.value());
    }
}
