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

package org.apache.ignite.internal.storage.rocksdb;

import static org.apache.ignite.internal.storage.rocksdb.PartitionDataHelper.setFirstBit;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.jupiter.api.Test;

class PartitionDataHelperTest {
    @Test
    void testSetFirstBit() {
        byte[] array = {1, 0, 42};

        setFirstBit(array, 1, true);

        assertThat(array, is(new byte[] {1, 1, 42}));

        setFirstBit(array, 1, true);

        assertThat(array, is(new byte[] {1, 1, 42}));

        setFirstBit(array, 1, false);

        assertThat(array, is(new byte[] {1, 0, 42}));

        setFirstBit(array, 1, false);

        assertThat(array, is(new byte[] {1, 0, 42}));

        setFirstBit(array, 2, true);

        assertThat(array, is(new byte[] {1, 0, 43}));

        setFirstBit(array, 2, true);

        assertThat(array, is(new byte[] {1, 0, 43}));

        setFirstBit(array, 2, false);

        assertThat(array, is(new byte[] {1, 0, 42}));

        setFirstBit(array, 2, false);

        assertThat(array, is(new byte[] {1, 0, 42}));
    }
}
