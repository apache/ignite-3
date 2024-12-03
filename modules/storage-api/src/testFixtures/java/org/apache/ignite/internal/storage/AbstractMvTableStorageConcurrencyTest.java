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

/**
 * Test to check for race conditions in MV table storage.
 */
@SuppressWarnings("JUnitTestMethodInProductSource")
public abstract class AbstractMvTableStorageConcurrencyTest extends BaseMvTableStorageTest {
    @RepeatedTest(100)
    void destroyTableAndPartitionAndIndexes() {
        getOrCreateMvPartition(tableStorage, PARTITION_ID);

        SortedIndexStorage sortedIndexStorage = tableStorage.getOrCreateSortedIndex(PARTITION_ID, sortedIdx);
        assertThat(sortedIndexStorage, is(notNullValue()));

        HashIndexStorage hashIndexStorage = tableStorage.getOrCreateHashIndex(PARTITION_ID, hashIdx);
        assertThat(hashIndexStorage, is(notNullValue()));

        runRace(
                () -> tableStorage.destroy().get(10, SECONDS),
                () -> tableStorage.destroyPartition(PARTITION_ID).get(10, SECONDS),
                () -> tableStorage.destroyIndex(sortedIdx.id()).get(10, SECONDS),
                () -> tableStorage.destroyIndex(hashIdx.id()).get(10, SECONDS)
        );
    }
}
