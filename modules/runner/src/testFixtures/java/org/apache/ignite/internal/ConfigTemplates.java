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

package org.apache.ignite.internal;

import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIMEM_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_ROCKSDB_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_TEST_PROFILE_NAME;

/**
 * Node configuration templates.
 */
public class ConfigTemplates {
    public static final String NL = System.lineSeparator();

    private static final String DEFAULT_PROFILES = ""
            + "  storage.profiles: {"
            + "        " + DEFAULT_TEST_PROFILE_NAME + ".engine: test, "
            + "        " + DEFAULT_AIPERSIST_PROFILE_NAME + ".engine: aipersist, "
            + "        " + DEFAULT_AIMEM_PROFILE_NAME + ".engine: aimem, "
            + "        " + DEFAULT_ROCKSDB_PROFILE_NAME + ".engine: rocksdb"
            + "  }," + NL;

    private static final String FAST_FAILURE_DETECTION = ""
            + "  network.membership: {" + NL
            + "    membershipSyncIntervalMillis: 1000," + NL
            + "    failurePingIntervalMillis: 500," + NL
            + "    scaleCube: {" + NL
            + "      membershipSuspicionMultiplier: 1," + NL
            + "      failurePingRequestMembers: 1," + NL
            + "      gossipIntervalMillis: 10" + NL
            + "    }" + NL
            + "  }," + NL;

    private static final String DISABLED_FAILURE_DETECTION = ""
            + "  network.membership: {" + NL
            + "    failurePingIntervalMillis: 1000000000" + NL
            + "  }" + NL;

    /** Default node bootstrap configuration pattern. */
    public static String NODE_BOOTSTRAP_CFG_TEMPLATE = renderConfigTemplate(DEFAULT_PROFILES);

    /** Template for tests that may not have some storage engines enabled. */
    public static String NODE_BOOTSTRAP_CFG_TEMPLATE_WITHOUT_STORAGE_PROFILES = renderConfigTemplate("");

    /** Template for node bootstrap config with Scalecube settings for fast failure detection. */
    public static final String FAST_FAILURE_DETECTION_NODE_BOOTSTRAP_CFG_TEMPLATE
            = renderConfigTemplate(DEFAULT_PROFILES + FAST_FAILURE_DETECTION);

    /** Template for node bootstrap config with Scalecube settings for disabled failure detection. */
    public static final String DISABLED_FAILURE_DETECTION_NODE_BOOTSTRAP_CFG_TEMPLATE
            = renderConfigTemplate(DEFAULT_PROFILES + DISABLED_FAILURE_DETECTION);

    /**
     * Renders the configuration template, adding the provided properties string to the template.
     *
     * @param properties Additional properties string.
     * @return Generated configuration template.
     */
    public static String renderConfigTemplate(String properties) {
        return "ignite {" + NL
                + "  network: {" + NL
                + "    port: {}," + NL
                + "    nodeFinder.netClusterNodes: [ {} ]" + NL
                + "  }," + NL
                + "  clientConnector.port: {}," + NL
                + "  clientConnector.sendServerExceptionStackTraceToClient: true," + NL
                + "  rest.port: {}," + NL
                + "  failureHandler.handler.type: noop," + NL
                + "  failureHandler.dumpThreadsOnFailure: false," + NL
                + properties
                + "}";
    }
}
