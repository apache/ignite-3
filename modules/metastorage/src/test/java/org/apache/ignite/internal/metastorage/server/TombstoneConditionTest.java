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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.impl.EntryImpl;
import org.junit.jupiter.api.Test;

/**
 * Tests for entry tombstone condition.
 *
 * @see TombstoneCondition
 */
public class TombstoneConditionTest {
    /** Entry key. */
    private static final byte[] KEY = new byte[]{1};

    /** Entry value. */
    private static final byte[] VAL = new byte[]{1};

    /** Regular entry. */
    private static final Entry ENTRY = new EntryImpl(KEY, VAL, 1, 1);

    /** Empty entry. */
    private static final Entry EMPTY = EntryImpl.empty(KEY);

    /** Tombstone entry. */
    private static final Entry TOMBSTONE = EntryImpl.tombstone(KEY, 1, 1);

    /**
     * Tests {@link TombstoneCondition.Type#TOMBSTONE} condition for regular, empty and tombstone entries.
     */
    @Test
    public void tombstone() {
        Condition cond = new TombstoneCondition(TombstoneCondition.Type.TOMBSTONE, KEY);

        assertFalse(cond.test(ENTRY));
        assertFalse(cond.test(EMPTY));
        assertTrue(cond.test(TOMBSTONE));
    }

    /**
     * Tests {@link TombstoneCondition.Type#NOT_TOMBSTONE} condition for regular, empty and tombstone entries.
     */
    @Test
    public void notTombstone() {
        Condition cond = new TombstoneCondition(TombstoneCondition.Type.NOT_TOMBSTONE, KEY);

        assertTrue(cond.test(ENTRY));
        assertTrue(cond.test(EMPTY));
        assertFalse(cond.test(TOMBSTONE));
    }
}
