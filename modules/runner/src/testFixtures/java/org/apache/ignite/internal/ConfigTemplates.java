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
    /**
     * Renders the configuration template, adding the provided properties string to the template.
     *
     * @param properties Additional properties string.
     * @return Generated configuration template.
     */
    public static String renderConfigTemplate(String properties) {
        return "ignite {\n"
                + "  network: {\n"
                + "    port: {},\n"
                + "    nodeFinder.netClusterNodes: [ {} ]\n"
                + "  },\n"
                + "  clientConnector.port: {},\n"
                + "  clientConnector.sendServerExceptionStackTraceToClient: true,\n"
                + "  rest.port: {},\n"
                + "  failureHandler.dumpThreadsOnFailure: false,\n"
                + properties
                + "}";
    }

    private static final String DEFAULT_PROFILES = ""
            + "  storage.profiles: {"
            + "        " + DEFAULT_TEST_PROFILE_NAME + ".engine: test, "
            + "        " + DEFAULT_AIPERSIST_PROFILE_NAME + ".engine: aipersist, "
            + "        " + DEFAULT_AIMEM_PROFILE_NAME + ".engine: aimem, "
            + "        " + DEFAULT_ROCKSDB_PROFILE_NAME + ".engine: rocksdb"
            + "  },\n";

    /** Default node bootstrap configuration pattern. */
    public static String NODE_BOOTSTRAP_CFG_TEMPLATE = renderConfigTemplate(DEFAULT_PROFILES);

    /** Template for tests that may not have some storage engines enabled. */
    public static String NODE_BOOTSTRAP_CFG_TEMPLATE_WITHOUT_STORAGE_PROFILES = renderConfigTemplate("");

    private static final String FAST_FAILURE_DETECTION = ""
            + "  network.membership: {\n"
            + "    membershipSyncIntervalMillis: 1000,\n"
            + "    failurePingIntervalMillis: 500,\n"
            + "    scaleCube: {\n"
            + "      membershipSuspicionMultiplier: 1,\n"
            + "      failurePingRequestMembers: 1,\n"
            + "      gossipIntervalMillis: 10\n"
            + "    }\n"
            + "  },\n";

    private static final String DISABLED_FAILURE_DETECTION = ""
            + "  network.membership: {\n"
            + "    failurePingIntervalMillis: 1000000000\n"
            + "  }\n";

    /** Template for node bootstrap config with Scalecube settings for fast failure detection. */
    public static final String FAST_FAILURE_DETECTION_NODE_BOOTSTRAP_CFG_TEMPLATE
            = renderConfigTemplate(DEFAULT_PROFILES + FAST_FAILURE_DETECTION);

    /** Template for node bootstrap config with Scalecube settings for disabled failure detection. */
    public static final String DISABLED_FAILURE_DETECTION_NODE_BOOTSTRAP_CFG_TEMPLATE
            = renderConfigTemplate(DEFAULT_PROFILES + DISABLED_FAILURE_DETECTION);
}
