/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.causality;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

/**
 * Tests of causality token implementation based on versioned value.
 * {@link VersionedValue}
 */
public class VersionValueTest {
    /** Test value. */
    public static final int TEST_VALUE = 1;

    /**
     * The test gets a value for {@link VersionedValue} before the value is calculated.
     *
     * @throws OutdatedTokenException If failed.
     */
    @Test
    public void testGetValueBeforeReady() throws OutdatedTokenException {
        VersionedValue<Integer> longVersionedValue = new VersionedValue<>((integerVersionedValue, token) -> {
            integerVersionedValue.set(token, TEST_VALUE);
        }, 2);

        CompletableFuture<Integer> fut = longVersionedValue.get(1);

        assertFalse(fut.isDone());

        longVersionedValue.onStorageRevisionUpdate(1);

        assertTrue(fut.isDone());

        assertEquals(TEST_VALUE, fut.join());

        assertSame(fut, longVersionedValue.get(1));
    }

    /**
     * The test explicitly sets a value to {@link VersionedValue} without waiting for the revision updaste.
     *
     * @throws OutdatedTokenException If failed.
     */
    @Test
    public void testExplicitlySetValue() throws OutdatedTokenException {
        VersionedValue<Integer> longVersionedValue = new VersionedValue<>();

        CompletableFuture<Integer> fut = longVersionedValue.get(1);

        assertFalse(fut.isDone());

        longVersionedValue.set(1, TEST_VALUE);

        assertTrue(fut.isDone());

        assertEquals(TEST_VALUE, fut.join());

        assertSame(fut, longVersionedValue.get(1));
    }

    /**
     * The test reads a value with the specific token in which the value should not be updated.
     * The read happenes before the revision updated.
     *
     * @throws OutdatedTokenException If failed.
     */
    @Test
    public void testMissValueUpdateBeforeReady() throws OutdatedTokenException {
        VersionedValue<Integer> longVersionedValue = new VersionedValue<>();

        longVersionedValue.set(1, TEST_VALUE);

        longVersionedValue.onStorageRevisionUpdate(1);

        CompletableFuture<Integer> fut = longVersionedValue.get(2);

        assertFalse(fut.isDone());

        longVersionedValue.onStorageRevisionUpdate(2);

        assertTrue(fut.isDone());

        assertEquals(TEST_VALUE, fut.join());

        assertSame(fut.join(), longVersionedValue.get(1).join());
    }

    /**
     * The test reads a value with the specific token in which the value should not be updated.
     * The read happenes after the revision updated.
     *
     * @throws OutdatedTokenException If failed.
     */
    @Test
    public void testMissValueUpdate() throws OutdatedTokenException {
        VersionedValue<Integer> longVersionedValue = new VersionedValue<>();

        longVersionedValue.set(1, TEST_VALUE);

        longVersionedValue.onStorageRevisionUpdate(1);
        longVersionedValue.onStorageRevisionUpdate(2);

        CompletableFuture<Integer> fut = longVersionedValue.get(2);

        assertTrue(fut.isDone());

        assertEquals(TEST_VALUE, fut.join());

        assertSame(fut, longVersionedValue.get(1));
    }

    /**
     * Test checks token history size.
     */
    @Test()
    public void testAbsoleteToken() {
        VersionedValue<Integer> longVersionedValue = new VersionedValue<>();

        longVersionedValue.set(1, TEST_VALUE);

        longVersionedValue.onStorageRevisionUpdate(1);

        longVersionedValue.set(2, TEST_VALUE);

        longVersionedValue.onStorageRevisionUpdate(2);
        longVersionedValue.onStorageRevisionUpdate(3);

        assertThrowsExactly(OutdatedTokenException.class, () -> longVersionedValue.get(1));
    }
}
