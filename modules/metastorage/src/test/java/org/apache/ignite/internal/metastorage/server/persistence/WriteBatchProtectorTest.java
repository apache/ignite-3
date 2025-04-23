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

package org.apache.ignite.internal.metastorage.server.persistence;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WriteBatchProtectorTest {
    private WriteBatchProtector protector;

    @BeforeEach
    void setUp() {
        protector = new WriteBatchProtector();
    }

    @Test
    void testUpdateKey() {
        byte[] key = "testKey".getBytes();

        protector.onUpdate(key);

        assertTrue(protector.maybeUpdated(key), "Key should be present in the bloom filter");
    }

    @Test
    void testNoUpdates() {
        byte[] key = "nonExistentKey".getBytes();

        assertFalse(protector.maybeUpdated(key), "Key should not be present in the bloom filter");
    }

    @Test
    void testUpdateAndClear() {
        byte[] key = "testKey".getBytes();

        protector.onUpdate(key);
        protector.clear();

        assertFalse(protector.maybeUpdated(key), "Key should not be present after clearing the bloom filter");
    }

    @Test
    void testUpdateMultipleKeys() {
        byte[] key1 = "key1".getBytes();
        byte[] key2 = "key2".getBytes();

        protector.onUpdate(key1);
        protector.onUpdate(key2);

        assertTrue(protector.maybeUpdated(key1), "Key1 should be present in the bloom filter");
        assertTrue(protector.maybeUpdated(key2), "Key2 should be present in the bloom filter");
    }

    @Test
    void testNoCollision() {
        byte[] key1 = "key1".getBytes();
        byte[] key2 = "key2".getBytes();

        protector.onUpdate(key1);

        // No collision.
        assertFalse(protector.maybeUpdated(key2), "Key2 should not be present unless explicitly added");
    }
}
