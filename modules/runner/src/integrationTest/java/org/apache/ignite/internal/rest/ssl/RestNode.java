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

package org.apache.ignite.internal.rest.ssl;

import java.nio.file.Path;
import org.apache.ignite.IgnitionManager;

/** Presentation of Ignite node for tests. */
public class RestNode {

    /** Key store path. */
    private static final String keyStorePath = "ssl/keystore.p12";

    /** Key store password. */
    private static final String keyStorePassword = "changeit";

    private final Path workDir;
    private final String name;
    private final int networkPort;
    private final int httpPort;
    private final int httpsPort;
    private final boolean sslEnabled;
    private final boolean dualProtocol;

    /** Constructor. */
    public RestNode(
            Path workDir,
            String name,
            int networkPort,
            int httpPort,
            int httpsPort,
            boolean sslEnabled,
            boolean dualProtocol
    ) {
        this.workDir = workDir;
        this.name = name;
        this.networkPort = networkPort;
        this.httpPort = httpPort;
        this.httpsPort = httpsPort;
        this.sslEnabled = sslEnabled;
        this.dualProtocol = dualProtocol;
    }

    public RestNode start() {
        IgnitionManager.start(name, bootstrapCfg(), workDir.resolve(name));
        return this;
    }

    public void stop() {
        IgnitionManager.stop(name);
    }

    public String httpAddress() {
        return "http://localhost:" + httpPort;
    }

    public String httpsAddress() {
        return "https://localhost:" + httpsPort;
    }

    private String bootstrapCfg() {
        String keyStoreAbsolutPath = ItRestSslTest.class.getClassLoader().getResource(keyStorePath).getPath();
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
                + "      port: " + httpsPort + ",\n"
                + "      keyStore: {\n"
                + "        path: " + keyStoreAbsolutPath + ",\n"
                    + "    password: " + keyStorePassword + "\n"
                + "      }\n"
                + "    }\n"
                + "  }"
                + "}";
    }
}
