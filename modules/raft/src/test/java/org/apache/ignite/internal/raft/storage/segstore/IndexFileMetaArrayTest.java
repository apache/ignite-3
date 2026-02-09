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
import static org.hamcrest.Matchers.nullValue;

import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

class IndexFileMetaArrayTest extends BaseIgniteAbstractTest {
    @Test
    void testAddGet() {
        var initialMeta = new IndexFileMeta(1, 2, 0, 0);

        var array = new IndexFileMetaArray(initialMeta);

        assertThat(array.size(), is(1));
        assertThat(array.get(0), is(initialMeta));
        assertThat(array.firstLogIndexInclusive(), is(1L));
        assertThat(array.lastLogIndexExclusive(), is(2L));

        var meta2 = new IndexFileMeta(2, 3, 0, 1);

        array = array.add(meta2);

        assertThat(array.size(), is(2));
        assertThat(array.get(1), is(meta2));
        assertThat(array.firstLogIndexInclusive(), is(1L));
        assertThat(array.lastLogIndexExclusive(), is(3L));

        for (int i = 0; i < INITIAL_CAPACITY; i++) {
            long logIndex = meta2.firstLogIndexInclusive() + i + 1;

            array = array.add(new IndexFileMeta(logIndex, logIndex + 1, 0, i + 2));
        }

        var meta3 = new IndexFileMeta(INITIAL_CAPACITY + 3, INITIAL_CAPACITY + 4, 0, INITIAL_CAPACITY + 3);

        array = array.add(meta3);

        assertThat(array.size(), is(3 + INITIAL_CAPACITY));
        assertThat(array.get(array.size() - 1), is(meta3));
        assertThat(array.firstLogIndexInclusive(), is(1L));
        assertThat(array.lastLogIndexExclusive(), is(INITIAL_CAPACITY + 4L));
    }

    @Test
    void testFindReturnsCorrectIndex() {
        var meta1 = new IndexFileMeta(1, 10, 100, 0);
        var meta2 = new IndexFileMeta(10, 20, 200, 1);
        var meta3 = new IndexFileMeta(20, 30, 300, 2);

        IndexFileMetaArray array = new IndexFileMetaArray(meta1)
                .add(meta2)
                .add(meta3);

        assertThat(array.find(0), is(nullValue()));

        assertThat(array.find(1), is(meta1));
        assertThat(array.find(5), is(meta1));
        assertThat(array.find(9), is(meta1));

        assertThat(array.find(10), is(meta2));
        assertThat(array.find(15), is(meta2));
        assertThat(array.find(19), is(meta2));

        assertThat(array.find(20), is(meta3));
        assertThat(array.find(25), is(meta3));
        assertThat(array.find(29), is(meta3));

        assertThat(array.find(30), is(nullValue()));
    }

    @Test
    void testFindReturnsNullForOutOfRange() {
        var meta = new IndexFileMeta(100, 200, 1000, 0);
        var array = new IndexFileMetaArray(meta);

        assertThat(array.find(99), is(nullValue()));
        assertThat(array.find(201), is(nullValue()));
    }

    @Test
    void testFindWorksCorrectlyWithEmptyMetas() {
        var meta1 = new IndexFileMeta(1, 10, 100, 0);
        var meta2 = new IndexFileMeta(10, 10, 200, 1);
        var meta3 = new IndexFileMeta(10, 20, 200, 2);

        IndexFileMetaArray array = new IndexFileMetaArray(meta1)
                .add(meta2)
                .add(meta3);

        assertThat(array.find(9), is(meta1));
        assertThat(array.find(10), is(meta3));
    }

    @Test
    void testTruncateInsideMetaAdjustsPayloadOffset() {
        var meta1 = new IndexFileMeta(1, 10, 100, 0);
        var meta2 = new IndexFileMeta(10, 20, 200, 1);

        IndexFileMetaArray array = new IndexFileMetaArray(meta1).add(meta2);

        IndexFileMetaArray truncated = array.truncateIndicesSmallerThan(5);

        assertThat(truncated.size(), is(2));

        IndexFileMeta trimmedMeta = truncated.get(0);

        assertThat(trimmedMeta.firstLogIndexInclusive(), is(5L));
        assertThat(trimmedMeta.lastLogIndexExclusive(), is(10L));
        assertThat(trimmedMeta.indexFilePayloadOffset(), is(100 + 4 * Integer.BYTES));

        assertThat(truncated.get(1), is(meta2));
    }

    @Test
    void testTruncateToMetaBoundarySkipsPreviousMeta() {
        var meta1 = new IndexFileMeta(1, 10, 100, 0);
        var meta2 = new IndexFileMeta(10, 20, 200, 1);

        IndexFileMetaArray array = new IndexFileMetaArray(meta1).add(meta2);

        IndexFileMetaArray truncated = array.truncateIndicesSmallerThan(10);

        assertThat(truncated.size(), is(1));
        assertThat(truncated.get(0), is(meta2));
    }
}
