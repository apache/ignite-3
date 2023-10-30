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

package org.apache.ignite.internal.index;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.internal.index.IndexManagementUtils.extractIndexIdFromPartitionBuildIndexKey;
import static org.apache.ignite.internal.index.IndexManagementUtils.extractPartitionIdFromPartitionBuildIndexKey;
import static org.apache.ignite.internal.index.IndexManagementUtils.inProgressBuildIndexMetastoreKey;
import static org.apache.ignite.internal.index.IndexManagementUtils.partitionBuildIndexMetastoreKey;
import static org.apache.ignite.internal.index.IndexManagementUtils.partitionBuildIndexMetastoreKeyPrefix;
import static org.apache.ignite.internal.index.IndexManagementUtils.toPartitionBuildIndexMetastoreKeyString;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.ignite.internal.lang.ByteArray;
import org.junit.jupiter.api.Test;

/** For {@link IndexManagementUtils} testing. */
public class IndexManagementUtilsTest {
    @Test
    void testPartitionBuildIndexMetastoreKey() {
        assertEquals(ByteArray.fromString("indexBuild.partition.1.2"), partitionBuildIndexMetastoreKey(1, 2));
        assertEquals(ByteArray.fromString("indexBuild.partition.7.9"), partitionBuildIndexMetastoreKey(7, 9));
    }

    @Test
    void testInProgressBuildIndexMetastoreKey() {
        assertEquals(ByteArray.fromString("indexBuild.inProgress.1"), inProgressBuildIndexMetastoreKey(1));
        assertEquals(ByteArray.fromString("indexBuild.inProgress.7"), inProgressBuildIndexMetastoreKey(7));
    }

    @Test
    void testPartitionBuildIndexMetastoreKeyPrefix() {
        assertEquals(ByteArray.fromString("indexBuild.partition.1"), partitionBuildIndexMetastoreKeyPrefix(1));
        assertEquals(ByteArray.fromString("indexBuild.partition.7"), partitionBuildIndexMetastoreKeyPrefix(7));
    }

    @Test
    void tesToPartitionBuildIndexMetastoreKeyString() {
        assertEquals("indexBuild.partition.1.2", toPartitionBuildIndexMetastoreKeyString("indexBuild.partition.1.2".getBytes(UTF_8)));
        assertEquals("indexBuild.partition.7.9", toPartitionBuildIndexMetastoreKeyString("indexBuild.partition.7.9".getBytes(UTF_8)));
    }

    @Test
    void testExtractPartitionIdFromPartitionBuildIndexKey() {
        assertEquals(2, extractPartitionIdFromPartitionBuildIndexKey("indexBuild.partition.1.2"));
        assertEquals(9, extractPartitionIdFromPartitionBuildIndexKey("indexBuild.partition.7.9"));
    }

    @Test
    void testExtractIndexIdFromPartitionBuildIndexKey() {
        assertEquals(1, extractIndexIdFromPartitionBuildIndexKey("indexBuild.partition.1.2"));
        assertEquals(7, extractIndexIdFromPartitionBuildIndexKey("indexBuild.partition.7.9"));
    }
}
