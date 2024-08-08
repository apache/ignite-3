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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.ignite.configuration.NodeConfiguration;
import org.apache.ignite.configuration.NodeConfigurationImpl;
import org.apache.ignite.failure.configuration.FailureProcessorBuilder;
import org.apache.ignite.failure.handlers.configuration.NoOpFailureHandlerBuilder;
import org.apache.ignite.failure.handlers.configuration.StopNodeFailureHandlerBuilder;
import org.apache.ignite.failure.handlers.configuration.StopNodeOrHaltFailureHandlerBuilder;
import org.apache.ignite.network.configuration.NetworkBuilder;
import org.apache.ignite.network.configuration.NodeFinderBuilder;
import org.junit.jupiter.api.Test;

class NodeConfigurationBuilderTest {
    @Test
    void network() {
        // Create config value builder and set array value
        NodeFinderBuilder nodeFinder = NodeFinderBuilder.create()
                .netClusterNodes("localhost:3344", "localhost:3345");
        // Create root config builder and set config values
        NetworkBuilder networkBuilder = NetworkBuilder.create()
                .nodeFinder(nodeFinder)
                .port(3344);
        // Create node configuration builder and set root config
        NodeConfiguration nodeConfiguration = NodeConfiguration.create()
                .network(networkBuilder);

        String configString = ((NodeConfigurationImpl) nodeConfiguration).build(null);
        Config config = ConfigFactory.parseString(configString);

        assertThat(config.getStringList("network.nodeFinder.netClusterNodes"), contains("localhost:3344"));
        assertThat(config.getString("network.nodeFinder.type"), is("STATIC"));
        assertThat(config.getInt("network.port"), is(3344));
    }

    @Test
    void noOpFailureHandler() {
        NodeConfiguration nodeConfiguration = NodeConfiguration.create().failureHandler(
                FailureProcessorBuilder.create().handler(
                        NoOpFailureHandlerBuilder.create()
                                .ignoredFailureTypes()
                ));

        String config = ((NodeConfigurationImpl) nodeConfiguration).build(null);

        assertThat(config, containsString(
                "failureHandler {\n"
                        + "    handler {\n"
                        + "        ignoredFailureTypes=[]\n"
                        + "        type=noop\n"
                        + "    }\n"
                        + "}\n"
        ));
    }

    @Test
    void stopNodeFailureHandler() {
        NodeConfiguration nodeConfiguration = NodeConfiguration.create().failureHandler(
                FailureProcessorBuilder.create().handler(StopNodeFailureHandlerBuilder.create())
        );

        String config = ((NodeConfigurationImpl) nodeConfiguration).build(null);

        assertThat(config, containsString(
                "failureHandler {\n"
                        + "    handler {\n"
                        + "        ignoredFailureTypes=[\n"
                        + "            systemWorkerBlocked,\n"
                        + "            systemCriticalOperationTimeout\n"
                        + "        ]\n"
                        + "        type=stop\n"
                        + "    }\n"
                        + "}\n"
        ));
    }

    @Test
    void stopNodeOrHaltFailureHandler() {
        NodeConfiguration nodeConfiguration = NodeConfiguration.create().failureHandler(
                FailureProcessorBuilder.create().handler(
                        StopNodeOrHaltFailureHandlerBuilder.create()
                                .ignoredFailureTypes("foo")
                                .timeoutMillis(1)
                ));

        String config = ((NodeConfigurationImpl) nodeConfiguration).build(null);

        assertThat(config, containsString(
                "failureHandler {\n"
                        + "    handler {\n"
                        + "        ignoredFailureTypes=[\n"
                        + "            foo\n"
                        + "        ]\n"
                        + "        timeoutMillis=1\n"
                        + "        tryStop=false\n"
                        + "        type=stopOrHalt\n"
                        + "    }\n"
                        + "}\n"
        ));
    }
}
