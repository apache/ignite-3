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

package org.apache.ignite.internal.raft.storage.segstore;

import static org.apache.ignite.internal.raft.storage.segstore.IndexFileMetaArray.INITIAL_CAPACITY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

class IndexFileMetaArrayTest extends BaseIgniteAbstractTest {
    @Test
    void testAddGet() {
        var initialMeta = new IndexFileMeta(1, 1, 0);

        var array = new IndexFileMetaArray(initialMeta);

        assertThat(array.size(), is(1));
        assertThat(array.get(0), is(initialMeta));

        var meta2 = new IndexFileMeta(2, 2, 0);

        array = array.add(meta2);

        assertThat(array.size(), is(2));
        assertThat(array.get(1), is(meta2));

        for (int i = 0; i < INITIAL_CAPACITY; i++) {
            long logIndex = meta2.firstLogIndex() + i + 1;

            array = array.add(new IndexFileMeta(logIndex, logIndex, 0));
        }

        var meta3 = new IndexFileMeta(INITIAL_CAPACITY + 3, INITIAL_CAPACITY + 3, 0);

        array = array.add(meta3);

        assertThat(array.size(), is(3 + INITIAL_CAPACITY));
        assertThat(array.get(array.size() - 1), is(meta3));
    }

    @Test
    void testFindReturnsCorrectIndex() {
        var meta1 = new IndexFileMeta(1, 10, 100);
        var meta2 = new IndexFileMeta(11, 20, 200);
        var meta3 = new IndexFileMeta(21, 30, 300);

        IndexFileMetaArray array = new IndexFileMetaArray(meta1)
                .add(meta2)
                .add(meta3);

        assertThat(array.find(0), is(-1));

        assertThat(array.find(1), is(0));
        assertThat(array.find(5), is(0));
        assertThat(array.find(10), is(0));

        assertThat(array.find(11), is(1));
        assertThat(array.find(15), is(1));
        assertThat(array.find(20), is(1));

        assertThat(array.find(21), is(2));
        assertThat(array.find(25), is(2));
        assertThat(array.find(30), is(2));

        assertThat(array.find(31), is(-1));
    }

    @Test
    void testFindReturnsMinusOneForOutOfRange() {
        var meta = new IndexFileMeta(100, 200, 1000);
        var array = new IndexFileMetaArray(meta);

        assertThat(array.find(99), is(-1));
        assertThat(array.find(201), is(-1));
    }
}
