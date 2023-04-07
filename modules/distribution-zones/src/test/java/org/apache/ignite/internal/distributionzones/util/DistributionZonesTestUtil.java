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

package org.apache.ignite.internal.distributionzones.util;

import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.distributionzones.DistributionZonesUtil;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.util.ByteUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Util class for methods for Distribution zones tests.
 */
public class DistributionZonesTestUtil {
    /**
     * Asserts data nodes from {@link DistributionZonesUtil#zoneDataNodesKey(int)}.
     *
     * @param zoneId Zone id.
     * @param clusterNodes Data nodes.
     * @param keyValueStorage Key-value storage.
     * @throws InterruptedException If thread was interrupted.
     */
    public static void assertDataNodesForZone(
            int zoneId,
            @Nullable Set<String> clusterNodes,
            KeyValueStorage keyValueStorage
    ) throws InterruptedException {
        assertValueInStorage(
                keyValueStorage,
                zoneDataNodesKey(zoneId).bytes(),
                value -> DistributionZonesUtil.dataNodes(fromBytes(value)),
                clusterNodes,
                2000
        );
    }

    /**
     * Asserts {@link DistributionZonesUtil#zoneScaleUpChangeTriggerKey(int)} revision.
     *
     * @param revision Revision.
     * @param zoneId Zone id.
     * @param keyValueStorage Key-value storage.
     * @throws InterruptedException If thread was interrupted.
     */
    public static void assertZoneScaleUpChangeTriggerKey(
            long revision,
            int zoneId,
            KeyValueStorage keyValueStorage
    ) throws InterruptedException {
        assertValueInStorage(
                keyValueStorage,
                zoneScaleUpChangeTriggerKey(zoneId).bytes(),
                ByteUtils::bytesToLong,
                revision,
                2000
        );
    }

    /**
     * Asserts {@link DistributionZonesUtil#zoneScaleDownChangeTriggerKey(int)} revision.
     *
     * @param revision Revision.
     * @param zoneId Zone id.
     * @param keyValueStorage Key-value storage.
     * @throws InterruptedException If thread was interrupted.
     */
    public static void assertZoneScaleDownChangeTriggerKey(
            long revision,
            int zoneId,
            KeyValueStorage keyValueStorage
    ) throws InterruptedException {
        assertValueInStorage(
                keyValueStorage,
                zoneScaleDownChangeTriggerKey(zoneId).bytes(),
                ByteUtils::bytesToLong,
                revision,
                2000
        );
    }

    /**
     * Asserts {@link DistributionZonesUtil#zonesChangeTriggerKey(int)} revision.
     *
     * @param revision Revision.
     * @param zoneId Zone id.
     * @param keyValueStorage Key-value storage.
     * @throws InterruptedException If thread was interrupted.
     */
    public static void assertZonesChangeTriggerKey(
            long revision,
            int zoneId,
            KeyValueStorage keyValueStorage
    ) throws InterruptedException {
        assertValueInStorage(
                keyValueStorage,
                zonesChangeTriggerKey(zoneId).bytes(),
                ByteUtils::bytesToLong,
                revision,
                1000
        );
    }

    /**
     * Asserts {@link DistributionZonesUtil#zonesLogicalTopologyKey()} value.
     *
     * @param clusterNodes Expected cluster nodes.
     * @param keyValueStorage Key-value storage.
     * @throws InterruptedException If thread was interrupted.
     */
    public static void assertLogicalTopology(
            @Nullable Set<LogicalNode> clusterNodes,
            KeyValueStorage keyValueStorage
    ) throws InterruptedException {
        Set<String> nodes = clusterNodes == null
                ? null
                : clusterNodes.stream().map(LogicalNode::name).collect(Collectors.toSet());

        assertValueInStorage(
                keyValueStorage,
                zonesLogicalTopologyKey().bytes(),
                ByteUtils::fromBytes,
                nodes,
                1000
        );
    }

    /**
     * Asserts {@link DistributionZonesUtil#zonesLogicalTopologyKey()} value.
     *
     * @param clusterNodes Expected cluster nodes' names.
     * @param keyValueStorage Key-value storage.
     * @throws InterruptedException If thread was interrupted.
     */
    public static void assertLogicalTopologyWithNodeNames(@Nullable Set<String> clusterNodes, KeyValueStorage keyValueStorage)
            throws InterruptedException {
        assertValueInStorage(
                keyValueStorage,
                zonesLogicalTopologyKey().bytes(),
                ByteUtils::fromBytes,
                clusterNodes,
                1000
        );
    }

    private static <T> void assertValueInStorage(
            KeyValueStorage keyValueStorage,
            byte[] key,
            Function<byte[], T> valueTransformer,
            @Nullable T expectedValue,
            long timeoutMillis
    ) throws InterruptedException {
        boolean success = waitForCondition(() -> {
            byte[] storageValue = keyValueStorage.get(key).value();

            T actualValue = storageValue == null ? null : valueTransformer.apply(storageValue);

            return Objects.equals(actualValue, expectedValue);
        }, timeoutMillis);

        // We do a second check simply to print a nice error message in case the condition above is not achieved.
        if (!success) {
            byte[] storageValue = keyValueStorage.get(key).value();

            assertThat(storageValue == null ? null : valueTransformer.apply(storageValue), is(expectedValue));
        }
    }
}
