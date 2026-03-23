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

package org.apache.ignite.internal.raft.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.ignite.raft.jraft.entity.LogId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TermCacheTest {
    private TermCache termCache;

    @BeforeEach
    void setUp() {
        termCache = new TermCache(4);
    }

    @Test
    void testAppendAndLookup() {
        termCache.append(new LogId(1, 1));
        termCache.append(new LogId(2, 2));
        termCache.append(new LogId(3, 3));

        assertEquals(-1, termCache.lookup(0));
        assertEquals(1, termCache.lookup(1));
        assertEquals(2, termCache.lookup(2));
        assertEquals(3, termCache.lookup(3));
        assertEquals(3, termCache.lookup(4));
    }

    @Test
    void testAppendSameTerm() {
        termCache.append(new LogId(1, 1));
        termCache.append(new LogId(2, 1));

        assertEquals(1, termCache.lookup(1));
        assertEquals(1, termCache.lookup(2));
    }

    @Test
    void testReset() {
        termCache.append(new LogId(1, 1));
        termCache.reset();

        assertEquals(-1, termCache.lookup(1));
    }

    @Test
    void testTruncateTail() {
        termCache.append(new LogId(1, 1));
        termCache.append(new LogId(2, 1));
        termCache.append(new LogId(3, 2));

        termCache.truncateTail(2);

        assertEquals(1, termCache.lookup(1));
        assertEquals(1, termCache.lookup(2));
        assertEquals(1, termCache.lookup(3));
    }

    @Test
    void testTruncateTailExactMatch() {
        termCache.append(new LogId(1, 1));
        termCache.append(new LogId(2, 2));
        termCache.append(new LogId(3, 2));

        termCache.truncateTail(2);

        assertEquals(1, termCache.lookup(1));
        assertEquals(1, termCache.lookup(2));
        assertEquals(1, termCache.lookup(3));
    }
}
