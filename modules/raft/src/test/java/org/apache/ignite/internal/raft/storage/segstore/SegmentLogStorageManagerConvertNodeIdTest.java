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

import static org.apache.ignite.internal.raft.storage.segstore.SegmentLogStorageManager.CMG_GROUP_ID;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentLogStorageManager.METASTORAGE_GROUP_ID;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentLogStorageManager.SPECIAL_GROUP_ID_OFFSET;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;

class SegmentLogStorageManagerConvertNodeIdTest extends BaseIgniteAbstractTest {
    private static final String METASTORAGE_GROUP = "metastorage_group";

    private static final String CMG_GROUP = "cmg_group";

    private static final String PART_SEPARATOR = "_part_";

    private static final String PEER_SEPARATOR = "-";

    private static String partitionNodeId(int objectId, int partitionId, int peerIdx) {
        return objectId + PART_SEPARATOR + partitionId + PEER_SEPARATOR + peerIdx;
    }

    private static String metastorageNodeId(int peerIdx) {
        return METASTORAGE_GROUP + PEER_SEPARATOR + peerIdx;
    }

    private static String cmgNodeId(int peerIdx) {
        return CMG_GROUP + PEER_SEPARATOR + peerIdx;
    }

    @Test
    void metastorageGroup() {
        assertThat(convertNodeId(metastorageNodeId(0)), equalTo(METASTORAGE_GROUP_ID));
        assertThat(convertNodeId(metastorageNodeId(1)), equalTo(METASTORAGE_GROUP_ID));
    }

    @Test
    void cmgGroup() {
        assertThat(convertNodeId(cmgNodeId(0)), equalTo(CMG_GROUP_ID));
        assertThat(convertNodeId(cmgNodeId(1)), equalTo(CMG_GROUP_ID));
    }

    @Test
    void zeroPartitionGroupId() {
        assertThat(convertNodeId(partitionNodeId(0, 0, 0)), equalTo(SPECIAL_GROUP_ID_OFFSET));
    }

    @Test
    void partitionGroup() {
        long result = convertNodeId(partitionNodeId(42, 7, 0));
        assertThat(result, equalTo((42L << 32 | 7) + SPECIAL_GROUP_ID_OFFSET));
        assertThat(result, greaterThan(0L));
    }

    @Test
    void partitionGroupMaxObjectId() {
        long result = convertNodeId(partitionNodeId(Integer.MAX_VALUE, 0, 0));
        assertThat(result, equalTo(((long) Integer.MAX_VALUE << 32) + SPECIAL_GROUP_ID_OFFSET));
        assertThat(result, greaterThan(0L));
    }

    @Test
    void partitionGroupMaxPartitionId() {
        long result = convertNodeId(partitionNodeId(0, Integer.MAX_VALUE, 0));
        assertThat(result, equalTo(Integer.toUnsignedLong(Integer.MAX_VALUE) + SPECIAL_GROUP_ID_OFFSET));
        assertThat(result, greaterThan(0L));
    }

    @Test
    void partitionGroupMaxBothIds() {
        long result = convertNodeId(partitionNodeId(Integer.MAX_VALUE, Integer.MAX_VALUE, 0));
        assertThat(result, equalTo(((long) Integer.MAX_VALUE << 32 | Integer.toUnsignedLong(Integer.MAX_VALUE)) + SPECIAL_GROUP_ID_OFFSET));
        assertThat(result, greaterThan(0L));
    }

    @Test
    void peerIndexIgnoredForPartitionGroups() {
        long result0 = convertNodeId(partitionNodeId(10, 5, 0));
        long result1 = convertNodeId(partitionNodeId(10, 5, 1));
        long result99 = convertNodeId(partitionNodeId(10, 5, 99));
        assertThat(result0, equalTo(result1));
        assertThat(result0, equalTo(result99));
        assertThat(result0, greaterThan(0L));
    }

    @Test
    void differentPartitionGroupsProduceUniqueIds() {
        Set<Long> ids = new HashSet<>();
        for (int objectId = 0; objectId < 1000; objectId++) {
            for (int partId = 0; partId < 1000; partId++) {
                long id = convertNodeId(partitionNodeId(objectId, partId, 0));
                assertThat(ids.add(id), is(true));
            }
        }
    }

    @Test
    void specialGroupsDoNotCollideWithPartitionGroups() {
        long meta = convertNodeId(metastorageNodeId(0));
        long cmg = convertNodeId(cmgNodeId(0));
        long partition = convertNodeId(partitionNodeId(0, 0, 0));

        assertThat(meta, not(equalTo(cmg)));
        assertThat(meta, not(equalTo(partition)));
        assertThat(cmg, not(equalTo(partition)));
    }

    @Test
    void invalidIdsFallsThrough() {
        // Missing peer index is a bug — IDs without it should not match any known group.
        long metaNoSuffix = convertNodeId(METASTORAGE_GROUP);
        long cmgNoSuffix = convertNodeId(CMG_GROUP);
        long partNoSuffix = convertNodeId("5" + PART_SEPARATOR + "3");
        long noHyphen = convertNodeId("some_random_string");
        long unknownWithPeer = convertNodeId("unknown_group" + PEER_SEPARATOR + 0);

        assertThat(metaNoSuffix, not(equalTo(METASTORAGE_GROUP_ID)));
        assertThat(cmgNoSuffix, not(equalTo(CMG_GROUP_ID)));
        assertThat(partNoSuffix, not(equalTo((5L << 32 | 3) + SPECIAL_GROUP_ID_OFFSET)));

        // Must be unique and positive.
        Set<Long> ids = Set.of(metaNoSuffix, cmgNoSuffix, partNoSuffix, noHyphen, unknownWithPeer);

        assertThat(ids, hasSize(5));
        for (long id : ids) {
            assertThat(id, greaterThan(0L));
        }
    }

    private static long convertNodeId(String nodeId) {
        return SegmentLogStorageManager.convertNodeId(nodeId);
    }
}
