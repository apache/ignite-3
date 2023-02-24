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

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgnitionManager;
import org.apache.ignite.internal.rest.ssl.ItRestSslTest;

/** Presentation of Ignite node for tests. */
public class RestNode {

    private final String keyStorePath;
    private final String keyStorePassword;
    private final String trustStorePath;
    private final String trustStorePassword;
    private final Path workDir;
    private final String name;
    private final int networkPort;
    private final int httpPort;
    private final int httpsPort;
    private final boolean sslEnabled;
    private final boolean sslClientAuthEnabled;
    private final boolean dualProtocol;
    private CompletableFuture<Ignite> igniteNodeFuture;

    /** Constructor. */
    public RestNode(
            String keyStorePath,
            String keyStorePassword,
            String trustStorePath,
            String trustStorePassword,
            Path workDir,
            String name,
            int networkPort,
            int httpPort,
            int httpsPort,
            boolean sslEnabled,
            boolean sslClientAuthEnabled,
            boolean dualProtocol
    ) {
        this.keyStorePath = keyStorePath;
        this.keyStorePassword = keyStorePassword;
        this.trustStorePath = trustStorePath;
        this.trustStorePassword = trustStorePassword;
        this.workDir = workDir;
        this.name = name;
        this.networkPort = networkPort;
        this.httpPort = httpPort;
        this.httpsPort = httpsPort;
        this.sslEnabled = sslEnabled;
        this.sslClientAuthEnabled = sslClientAuthEnabled;
        this.dualProtocol = dualProtocol;
    }

    public static RestNodeBuilder builder() {
        return new RestNodeBuilder();
    }

    /** Starts the node. */
    public CompletableFuture<Ignite> start() {
        igniteNodeFuture = IgnitionManager.start(name, bootstrapCfg(), workDir.resolve(name));
        return igniteNodeFuture;
    }

    /** Restarts the node. */
    public CompletableFuture<Ignite> restart() {
        stop();
        igniteNodeFuture = IgnitionManager.start(name, null, workDir.resolve(name));
        return igniteNodeFuture;
    }

    /** Stops the node. */
    public void stop() {
        IgnitionManager.stop(name);
    }

    /** Returns the node name. */
    public String name() {
        return name;
    }

    /** Returns HTTP address of the node. Uses the port that was used in the config. */
    public String httpAddress() {
        return "http://localhost:" + httpPort;
    }

    /** Returns HTTPS address of the node. Uses the port that was used in the config. */
    public String httpsAddress() {
        return "https://localhost:" + httpsPort;
    }

    /** Returns future of the node. */
    public CompletableFuture<Ignite> igniteNodeFuture() {
        return igniteNodeFuture;
    }

    private String bootstrapCfg() {
        String keyStoreAbsolutPath = ItRestSslTest.class.getClassLoader().getResource(keyStorePath).getPath();
        String trustStoreAbsolutPath = ItRestSslTest.class.getClassLoader().getResource(trustStorePath).getPath();

        return "{\n"
                + "  network: {\n"
                + "    port: " + networkPort + ",\n"
                + "    nodeFinder: {\n"
                + "      netClusterNodes: [ \"localhost:3344\", \"localhost:3345\", \"localhost:3346\" ]\n"
                + "    }\n"
                + "  },\n"
                + "  rest: {\n"
                + "    port: " + httpPort + ",\n"
                + "    dualProtocol: " + dualProtocol + ",\n"
                + "    ssl: {\n"
                + "      enabled: " + sslEnabled + ",\n"
                + "      clientAuth: " + (sslClientAuthEnabled ? "require" : "none") + ",\n"
                + "      port: " + httpsPort + ",\n"
                + "      keyStore: {\n"
                + "        path: " + keyStoreAbsolutPath + ",\n"
                + "        password: " + keyStorePassword + "\n"
                + "      }, \n"
                + "      trustStore: {\n"
                + "        type: JKS, "
                + "        path: " + trustStoreAbsolutPath + ",\n"
                + "        password: " + trustStorePassword + "\n"
                + "      }\n"
                + "    }\n"
                + "  }"
                + "}";
    }
}
