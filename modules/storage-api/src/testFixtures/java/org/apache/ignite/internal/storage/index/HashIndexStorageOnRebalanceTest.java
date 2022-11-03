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

package org.apache.ignite.internal.storage.index;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.ignite.internal.schema.BinaryTuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * For {@link HashIndexStorageOnRebalance} testing.
 */
@ExtendWith(MockitoExtension.class)
public class HashIndexStorageOnRebalanceTest {
    @Mock
    private HashIndexStorage hashIndexStorage;

    private HashIndexStorageOnRebalance hashIndexStorageOnRebalance;

    @BeforeEach
    void setUp() {
        hashIndexStorageOnRebalance = new HashIndexStorageOnRebalance(hashIndexStorage);
    }

    @Test
    void testGet() {
        assertThrows(IllegalStateException.class, () -> hashIndexStorageOnRebalance.get(mock(BinaryTuple.class)));
    }

    @Test
    void testPut() {
        IndexRow indexRow = mock(IndexRow.class);

        hashIndexStorageOnRebalance.put(indexRow);

        verify(hashIndexStorage, times(1)).put(eq(indexRow));
    }

    @Test
    void testRemove() {
        IndexRow indexRow = mock(IndexRow.class);

        hashIndexStorageOnRebalance.remove(indexRow);

        verify(hashIndexStorage, times(1)).remove(eq(indexRow));
    }

    @Test
    void testIndexDescriptor() {
        hashIndexStorageOnRebalance.indexDescriptor();

        verify(hashIndexStorage, times(1)).indexDescriptor();
    }

    @Test
    void testDestroy() {
        hashIndexStorageOnRebalance.destroy();

        verify(hashIndexStorage, times(1)).destroy();
    }
}
