/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.configuration.sample;

import org.apache.ignite.configuration.ConfigurationRegistry;
import org.apache.ignite.configuration.sample.storage.TestConfigurationStorage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Simple usage test of generated configuration schema.
 */
public class UsageTest {
    /**
     * Test creation of configuration and calling configuration API methods.
     */
    @Test
    public void test() throws Exception {
        var registry = new ConfigurationRegistry();

        registry.registerRootKey(LocalConfiguration.KEY);

        registry.registerStorage(new TestConfigurationStorage());

        LocalConfiguration root = registry.getConfiguration(LocalConfiguration.KEY);

        root.change(local ->
            local.changeBaseline(baseline ->
                baseline.changeNodes(nodes ->
                    nodes.create("node1", node ->
                        node.initConsistentId("test").initPort(1000)
                    )
                ).changeAutoAdjust(autoAdjust ->
                    autoAdjust.changeEnabled(true).changeTimeout(100_000L)
                )
            )
        ).get();

        assertTrue(root.baseline().autoAdjust().enabled().value());

//        assertThrows(ConfigurationValidationException.class, () -> {
//            configurator.set(Selectors.LOCAL_BASELINE_AUTO_ADJUST_ENABLED, false);
//        });
//        configurator.set(Selectors.LOCAL_BASELINE_AUTO_ADJUST, new ChangeAutoAdjust().withEnabled(false).withTimeout(0L));

        root.baseline().nodes().get("node1").autoAdjustEnabled().change(false).get();
        root.baseline().autoAdjust().enabled().change(true).get();
        root.baseline().nodes().get("node1").autoAdjustEnabled().change(true).get();

        // ???
//        assertThrows(ExecutionException.class, () -> {
//            root.baseline().autoAdjust().enabled().change(false).get();
//        });
    }

    /**
     * Test to show an API to work with multiroot configurations.
     */
    @Test
    public void multiRootConfigurationTest() {
        ConfigurationRegistry sysConf = new ConfigurationRegistry();

        int failureDetectionTimeout = 30_000;
        int joinTimeout = 10_000;

        long autoAdjustTimeout = 30_000L;

//        InitNetwork initNetwork = new InitNetwork().withDiscovery(
//            new InitDiscovery()
//                .withFailureDetectionTimeout(failureDetectionTimeout)
//                .withJoinTimeout(joinTimeout)
//        );
//
//        InitLocal initLocal = new InitLocal().withBaseline(
//            new InitBaseline().withAutoAdjust(
//                new InitAutoAdjust().withEnabled(true)
//                    .withTimeout(autoAdjustTimeout))
//        );

//        Configurator<LocalConfigurationImpl> localConf = Configurator.create(LocalConfigurationImpl::new, initLocal);
//
//        sysConf.registerConfigurator(localConf);
//
//        Configurator<NetworkConfigurationImpl> networkConf = Configurator.create(NetworkConfigurationImpl::new, initNetwork);
//
//        sysConf.registerConfigurator(networkConf);
//
//        assertEquals(failureDetectionTimeout,
//            sysConf.getConfiguration(NetworkConfigurationImpl.KEY).discovery().failureDetectionTimeout().value());
//
//        assertEquals(autoAdjustTimeout,
//            sysConf.getConfiguration(LocalConfigurationImpl.KEY).baseline().autoAdjust().timeout().value());
    }
}
