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

package org.apache.ignite.internal.failure.configuration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

import org.apache.ignite.configuration.NodeConfiguration;
import org.apache.ignite.configuration.NodeConfigurationImpl;
import org.apache.ignite.failure.configuration.FailureProcessorConfigurationBuilder;
import org.junit.jupiter.api.Test;

class FailureProcessorConfigurationBuilderTest {
    @Test
    void noOpFailureHandler() {
        NodeConfiguration nodeConfiguration = NodeConfiguration.create();
        nodeConfiguration.withFailureHandler().withNoOpFailureHandler()
                .setIgnoredFailureTypes(new String[]{});

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
        NodeConfiguration nodeConfiguration = NodeConfiguration.create();
        nodeConfiguration.withFailureHandler().withStopNodeFailureHandler();

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
        NodeConfiguration nodeConfiguration = NodeConfiguration.create();
        FailureProcessorConfigurationBuilder builder = nodeConfiguration.withFailureHandler();
        builder.withStopNodeOrHaltFailureHandler()
                .setTimeoutMillis(1);

        String config = ((NodeConfigurationImpl) nodeConfiguration).build(null);

        assertThat(config, containsString(
                "failureHandler {\n"
                        + "    handler {\n"
                        + "        ignoredFailureTypes=[\n"
                        + "            systemWorkerBlocked,\n"
                        + "            systemCriticalOperationTimeout\n"
                        + "        ]\n"
                        + "        timeoutMillis=1\n"
                        + "        tryStop=false\n"
                        + "        type=stopOrHalt\n"
                        + "    }\n"
                        + "}\n"
        ));
    }
}
