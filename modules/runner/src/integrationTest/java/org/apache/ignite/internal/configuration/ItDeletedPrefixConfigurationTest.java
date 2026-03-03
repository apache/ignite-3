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

package org.apache.ignite.internal.configuration;

import static org.apache.ignite.internal.ConfigTemplates.NODE_BOOTSTRAP_CFG_TEMPLATE;
import static org.apache.ignite.internal.configuration.TestDistributedDeletedPrefixConfigurationModule.DELETED_DISTRIBUTED_PREFIX;
import static org.apache.ignite.internal.configuration.TestLocalDeletedPrefixConfigurationModule.DELETED_LOCAL_PREFIX;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.KeyIgnorer;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.configuration.presentation.HoconPresentation;
import org.junit.jupiter.api.Test;

/**
 * Integration tests verifying that {@link IgniteImpl} correctly wires deleted prefixes from each {@link ConfigurationModule}
 * into the corresponding {@link ConfigurationRegistry} via {@link ConfigurationRegistry#create}.
 */
public class ItDeletedPrefixConfigurationTest extends ClusterPerClassIntegrationTest {
    @Override
    protected int initialNodes() {
        return 1;
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return NODE_BOOTSTRAP_CFG_TEMPLATE.substring(0, NODE_BOOTSTRAP_CFG_TEMPLATE.lastIndexOf('}'))
                + "  testDeletedLocalProp = \"old_value\"\n}";
    }

    /**
     * Verifies that the local configuration registry's {@link KeyIgnorer} recognizes the local deleted prefix
     * but not the distributed deleted prefix.
     */
    @Test
    void testLocalRegistryKeyIgnorerRecognizesLocalDeletedPrefix() {
        ConfigurationRegistry nodeConfig = igniteImpl(0).nodeConfiguration();

        assertTrue(nodeConfig.keyIgnorer().shouldIgnore(DELETED_LOCAL_PREFIX),
                "Local registry must ignore local deleted prefix");
        assertFalse(nodeConfig.keyIgnorer().shouldIgnore(DELETED_DISTRIBUTED_PREFIX),
                "Local registry must NOT ignore distributed deleted prefix");
    }

    /**
     * Verifies that the distributed configuration registry's {@link KeyIgnorer} recognizes the distributed deleted prefix
     * but not the local deleted prefix.
     */
    @Test
    void testDistributedRegistryKeyIgnorerRecognizesDistributedDeletedPrefix() {
        ConfigurationRegistry clusterConfig = igniteImpl(0).clusterConfiguration();

        assertTrue(clusterConfig.keyIgnorer().shouldIgnore(DELETED_DISTRIBUTED_PREFIX),
                "Distributed registry must ignore distributed deleted prefix");
        assertFalse(clusterConfig.keyIgnorer().shouldIgnore(DELETED_LOCAL_PREFIX),
                "Distributed registry must NOT ignore local deleted prefix");
    }

    /**
     * Verifies that the node starts and the cluster initializes with deleted properties in both local and distributed configs.
     */
    @Test
    void testNodeStartsAndInitializesWithDeletedProperties() {
        // Verify the deleted local property was dropped and is absent from local config.
        String localConfigHocon = new HoconPresentation(igniteImpl(0).nodeConfiguration()).represent();
        assertFalse(localConfigHocon.contains("testDeletedLocalProp"),
                "Deleted local property must not be present in local config after startup");

        // Verify that applying the deleted distributed property via HoconPresentation succeeds.
        HoconPresentation clusterPresentation = new HoconPresentation(igniteImpl(0).clusterConfiguration());
        assertThat(clusterPresentation.update("ignite.testDeletedDistribProp = \"old_value\""),
                willCompleteSuccessfully());

        // Verify the deleted distributed property was dropped and is absent from distributed config.
        String clusterConfigHocon = clusterPresentation.represent();
        assertFalse(clusterConfigHocon.contains("testDeletedDistribProp"),
                "Deleted distributed property must not be present in distributed config after update");
    }
}
