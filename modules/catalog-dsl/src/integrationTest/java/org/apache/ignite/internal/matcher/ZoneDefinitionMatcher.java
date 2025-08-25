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

package org.apache.ignite.internal.matcher;

import static org.hamcrest.Matchers.equalTo;

import org.apache.ignite.catalog.definitions.ZoneDefinition;
import org.apache.ignite.internal.testframework.matchers.AnythingMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * Matcher implementation for {@link ZoneDefinition}.
 */
public class ZoneDefinitionMatcher extends TypeSafeMatcher<ZoneDefinition> {
    private Matcher<String> zoneNameMatcher = AnythingMatcher.anything();

    private Matcher<Boolean> ifNotExistsMatcher = AnythingMatcher.anything();

    private Matcher<Integer> partitionsMatcher = AnythingMatcher.anything();

    private Matcher<Integer> replicasMatcher = AnythingMatcher.anything();

    private Matcher<Integer> quorumSizeMatcher = AnythingMatcher.anything();

    private Matcher<String> distributionAlgorithmMatcher = AnythingMatcher.anything();

    private Matcher<Integer> dataNodesAutoAdjustScaleUpMatcher = AnythingMatcher.anything();

    private Matcher<Integer> dataNodesAutoAdjustScaleDownMatcher = AnythingMatcher.anything();

    private Matcher<String> filterMatcher = AnythingMatcher.anything();

    private Matcher<String> storageProfiles = AnythingMatcher.anything();

    private Matcher<String> consistencyMode = AnythingMatcher.anything();

    public ZoneDefinitionMatcher withZoneNameMatcher(Matcher<String> zoneNameMatcher) {
        this.zoneNameMatcher = zoneNameMatcher;
        return this;
    }

    public ZoneDefinitionMatcher withZoneName(String zoneName) {
        return withZoneNameMatcher(equalTo(zoneName));
    }

    public ZoneDefinitionMatcher withIfNotExistsMatcher(Matcher<Boolean> ifNotExistsMatcher) {
        this.ifNotExistsMatcher = ifNotExistsMatcher;
        return this;
    }

    public ZoneDefinitionMatcher withIfNotExists(boolean ifNotExists) {
        return withIfNotExistsMatcher(equalTo(ifNotExists));
    }

    public ZoneDefinitionMatcher withPartitionsMatcher(Matcher<Integer> partitionsMatcher) {
        this.partitionsMatcher = partitionsMatcher;
        return this;
    }

    public ZoneDefinitionMatcher withPartitions(int partitions) {
        return withPartitionsMatcher(equalTo(partitions));
    }

    public ZoneDefinitionMatcher withReplicasMatcher(Matcher<Integer> replicasMatcher) {
        this.replicasMatcher = replicasMatcher;
        return this;
    }

    public ZoneDefinitionMatcher withReplicas(int replicas) {
        return withReplicasMatcher(equalTo(replicas));
    }

    public ZoneDefinitionMatcher withQuorumSize(Matcher<Integer> quorumSizeMatcher) {
        this.quorumSizeMatcher = quorumSizeMatcher;
        return this;
    }

    public ZoneDefinitionMatcher withQuorumSize(int quorumSize) {
        return withQuorumSize(equalTo(quorumSize));
    }

    public ZoneDefinitionMatcher withDistributionAlgorithmMatcher(Matcher<String> withDistributionAlgorithmMatcher) {
        this.distributionAlgorithmMatcher = withDistributionAlgorithmMatcher;
        return this;
    }

    public ZoneDefinitionMatcher withDistributionAlgorithm(String distributionAlgorithm) {
        return withDistributionAlgorithmMatcher(equalTo(distributionAlgorithm));
    }

    public ZoneDefinitionMatcher withDataNodesAutoAdjustScaleUpMatcher(
            Matcher<Integer> dataNodesAutoAdjustScaleUpMatcher) {
        this.dataNodesAutoAdjustScaleUpMatcher = dataNodesAutoAdjustScaleUpMatcher;
        return this;
    }

    public ZoneDefinitionMatcher withDataNodesAutoAdjustScaleUp(int dataNodesAutoAdjustScaleUp) {
        return withDataNodesAutoAdjustScaleUpMatcher(equalTo(dataNodesAutoAdjustScaleUp));
    }

    public ZoneDefinitionMatcher withDataNodesAutoAdjustScaleDownMatcher(
            Matcher<Integer> dataNodesAutoAdjustScaleDownMatcher) {
        this.dataNodesAutoAdjustScaleDownMatcher = dataNodesAutoAdjustScaleDownMatcher;
        return this;
    }

    public ZoneDefinitionMatcher withDataNodesAutoAdjustScaleDown(int dataNodesAutoAdjustScaleDown) {
        return withDataNodesAutoAdjustScaleDownMatcher(equalTo(dataNodesAutoAdjustScaleDown));
    }

    public ZoneDefinitionMatcher withFilterMatcher(Matcher<String> filterMatcher) {
        this.filterMatcher = filterMatcher;
        return this;
    }

    public ZoneDefinitionMatcher withFilter(String filter) {
        return withFilterMatcher(equalTo(filter));
    }

    public ZoneDefinitionMatcher withStorageProfilesMatcher(Matcher<String> storageProfiles) {
        this.storageProfiles = storageProfiles;
        return this;
    }

    public ZoneDefinitionMatcher withStorageProfiles(String storageProfiles) {
        return withStorageProfilesMatcher(equalTo(storageProfiles));
    }

    public ZoneDefinitionMatcher withConsistencyModeMatcher(Matcher<String> consistenyMode) {
        this.consistencyMode = consistenyMode;
        return this;
    }

    public ZoneDefinitionMatcher withConsistencyMode(String consistencyMode) {
        return withConsistencyModeMatcher(equalTo(consistencyMode));
    }

    public static ZoneDefinitionMatcher isZoneDefinition() {
        return new ZoneDefinitionMatcher();
    }

    @Override
    protected boolean matchesSafely(ZoneDefinition zoneDefinition) {
        return zoneNameMatcher.matches(zoneDefinition.zoneName())
                && ifNotExistsMatcher.matches(zoneDefinition.ifNotExists())
                && partitionsMatcher.matches(zoneDefinition.partitions())
                && replicasMatcher.matches(zoneDefinition.replicas())
                && quorumSizeMatcher.matches(zoneDefinition.quorumSize())
                && distributionAlgorithmMatcher.matches(zoneDefinition.distributionAlgorithm())
                && zoneNameMatcher.matches(zoneDefinition.zoneName())
                && dataNodesAutoAdjustScaleUpMatcher.matches(zoneDefinition.dataNodesAutoAdjustScaleUp())
                && dataNodesAutoAdjustScaleDownMatcher.matches(zoneDefinition.dataNodesAutoAdjustScaleDown())
                && filterMatcher.matches(zoneDefinition.filter())
                && storageProfiles.matches(zoneDefinition.storageProfiles())
                && consistencyMode.matches(zoneDefinition.consistencyMode());
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("Zone definition with ")
                .appendText("zone name ").appendDescriptionOf(zoneNameMatcher)
                .appendText(System.lineSeparator())
                .appendText("if not exists ").appendDescriptionOf(ifNotExistsMatcher)
                .appendText(System.lineSeparator())
                .appendText("partitions ").appendDescriptionOf(partitionsMatcher)
                .appendText(System.lineSeparator())
                .appendText("replicas ").appendDescriptionOf(replicasMatcher)
                .appendText(System.lineSeparator())
                .appendText("quorum size ").appendDescriptionOf(quorumSizeMatcher)
                .appendText(System.lineSeparator())
                .appendText("affinity ").appendDescriptionOf(distributionAlgorithmMatcher)
                .appendText(System.lineSeparator())
                .appendText("data nodes auto adjust scale up ").appendDescriptionOf(dataNodesAutoAdjustScaleUpMatcher)
                .appendText(System.lineSeparator())
                .appendText("data nodes auto adjust scale down ").appendDescriptionOf(dataNodesAutoAdjustScaleDownMatcher)
                .appendText(System.lineSeparator())
                .appendText("filter ").appendDescriptionOf(filterMatcher)
                .appendText(System.lineSeparator())
                .appendText("storage profiles ").appendDescriptionOf(storageProfiles)
                .appendText(System.lineSeparator())
                .appendText("consistency mode ").appendDescriptionOf(consistencyMode);
    }
}
