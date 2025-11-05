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

package org.apache.ignite.internal.rest;

import static org.apache.ignite.internal.testframework.IgniteTestUtils.escapeWindowsPath;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.getResourcePath;

import org.apache.ignite.internal.Cluster;

/** Presentation of Ignite node for tests. */
public class RestNode {

    private final Cluster cluster;
    private final int index;
    private final String keyStoreFilePath;
    private final String keyStorePassword;
    private final String trustStoreFilePath;
    private final String trustStorePassword;
    private final boolean sslEnabled;
    private final boolean sslClientAuthEnabled;
    private final boolean dualProtocol;
    private final String ciphers;

    /** Constructor. */
    public RestNode(
            Cluster cluster,
            int index,
            String keyStorePath,
            String keyStorePassword,
            String trustStorePath,
            String trustStorePassword,
            boolean sslEnabled,
            boolean sslClientAuthEnabled,
            boolean dualProtocol,
            String ciphers
    ) {
        this.cluster = cluster;
        this.index = index;
        keyStoreFilePath = escapeWindowsPath(getResourcePath(RestNode.class, keyStorePath));
        this.keyStorePassword = keyStorePassword;
        trustStoreFilePath = escapeWindowsPath(getResourcePath(RestNode.class, trustStorePath));
        this.trustStorePassword = trustStorePassword;
        this.sslEnabled = sslEnabled;
        this.sslClientAuthEnabled = sslClientAuthEnabled;
        this.dualProtocol = dualProtocol;
        this.ciphers = ciphers;
    }

    public static RestNodeBuilder builder() {
        return new RestNodeBuilder();
    }

    /** Starts the node. */
    public void start() {
        cluster.startEmbeddedNode(index, bootstrapCfg());
    }

    /** Returns HTTP address of the node. Uses the port that was used in the config. */
    public String httpAddress() {
        return "http://localhost:" + cluster.httpPort(index);
    }

    /** Returns HTTPS address of the node. Uses the port that was used in the config. */
    public String httpsAddress() {
        return "https://localhost:" + cluster.httpsPort(index);
    }

    private String bootstrapCfg() {
        return "ignite {\n"
                + "  network.port: {},\n"
                + "  network.nodeFinder.netClusterNodes: [ {} ],\n"
                + "  clientConnector.port: {},\n"
                + "  rest: {\n"
                + "    port: {},\n"
                + "    dualProtocol: " + dualProtocol + ",\n"
                + "    ssl: {\n"
                + "      enabled: " + sslEnabled + ",\n"
                + "      clientAuth: " + (sslClientAuthEnabled ? "require" : "none") + ",\n"
                + "      ciphers: \"" + ciphers + "\",\n"
                + "      port: {},\n"
                + "      keyStore: {\n"
                + "        path: \"" + keyStoreFilePath + "\",\n"
                + "        password: " + keyStorePassword + "\n"
                + "      }, \n"
                + "      trustStore: {\n"
                + "        type: JKS,\n"
                + "        path: \"" + trustStoreFilePath + "\",\n"
                + "        password: " + trustStorePassword + "\n"
                + "      }\n"
                + "    }\n"
                + "  },\n"
                + "  failureHandler.dumpThreadsOnFailure: false\n"
                + "}";
    }
}
