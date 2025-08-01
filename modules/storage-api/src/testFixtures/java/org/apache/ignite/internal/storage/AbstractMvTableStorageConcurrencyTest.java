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

package org.apache.ignite.internal.storage;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runRace;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import org.apache.ignite.internal.storage.index.HashIndexStorage;
import org.apache.ignite.internal.storage.index.SortedIndexStorage;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/**
 * Test to check for race conditions in MV table storage.
 */
@SuppressWarnings("JUnitTestMethodInProductSource")
public abstract class AbstractMvTableStorageConcurrencyTest extends BaseMvTableStorageTest {
    /**
     * To be used in a loop. {@link RepeatedTest} causes engine to be recreated on each test, which takes too much time for 100 iterations.
     */
    private static final int REPEATS = 100;

    @Test
    void destroyTableAndPartitionAndIndexes() throws Exception {
        for (int i = 0; i < REPEATS; i++) {
            recreateTableStorage();

            getOrCreateMvPartition(tableStorage, PARTITION_ID);

            tableStorage.createSortedIndex(PARTITION_ID, sortedIdx);
            SortedIndexStorage sortedIndexStorage = (SortedIndexStorage) tableStorage.getIndex(PARTITION_ID, sortedIdx.id());
            assertThat(sortedIndexStorage, is(notNullValue()));

            tableStorage.createHashIndex(PARTITION_ID, hashIdx);
            HashIndexStorage hashIndexStorage = (HashIndexStorage) tableStorage.getIndex(PARTITION_ID, hashIdx.id());
            assertThat(hashIndexStorage, is(notNullValue()));

            runRace(
                    () -> tableStorage.destroy().get(10, SECONDS),
                    () -> tableStorage.destroyPartition(PARTITION_ID).get(10, SECONDS),
                    () -> tableStorage.destroyIndex(sortedIdx.id()).get(10, SECONDS),
                    () -> tableStorage.destroyIndex(hashIdx.id()).get(10, SECONDS)
            );
        }
    }
}
