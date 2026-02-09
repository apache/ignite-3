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

import static org.apache.ignite.internal.ConfigTemplates.NL;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.escapeWindowsPath;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.getResourcePath;
import static org.apache.ignite.internal.util.StringUtils.nullOrBlank;

import org.jetbrains.annotations.Nullable;

/** Helper class which provides node configuration template for SSL. */
public class NodeConfig {
    private static final String keyStorePath = "ssl/keystore.p12";
    private static final String trustStorePath = "ssl/truststore.jks";

    public static final String resolvedKeystorePath = getResourcePath(NodeConfig.class, keyStorePath);
    public static final String resolvedTruststorePath = getResourcePath(NodeConfig.class, trustStorePath);

    public static final String keyStorePassword = "changeit";
    public static final String trustStorePassword = "changeit";

    /** Node bootstrap configuration pattern with REST SSL enabled. */
    public static final String REST_SSL_BOOTSTRAP_CONFIG = restSslBootstrapConfig(null);

    /**
     *  Node bootstrap configuration pattern with REST SSL enabled.
     *
     * @param ciphers Custom ciphers suites.
     * @return Config pattern.
     */
    public static String restSslBootstrapConfig(@Nullable String ciphers) {
        return "ignite {" + NL
                + "  network: {" + NL
                + "    port: {}," + NL
                + "    nodeFinder: {" + NL
                + "      netClusterNodes: [ {} ]" + NL
                + "    }," + NL
                + "  }," + NL
                + "  clientConnector.port: {} ," + NL
                + "  rest: {" + NL
                + "    port: {}" + NL
                + "    ssl: {" + NL
                + "      port: {}," + NL
                + "      enabled: true," + NL
                + "      keyStore: {" + NL
                + "        path: \"" + escapeWindowsPath(resolvedKeystorePath) + "\"," + NL
                + "        password: " + keyStorePassword + NL
                + "      }," + NL
                + "      trustStore: {" + NL
                + "        path: \"" + escapeWindowsPath(resolvedTruststorePath) + "\"," + NL
                + "        password: " + trustStorePassword + NL
                + "      }," + NL
                + (nullOrBlank(ciphers) ? "" : "      ciphers: \"" + ciphers + "\"")
                + "    }" + NL
                + "  }," + NL
                + "  failureHandler.handler.type: noop,"  + NL
                + "  failureHandler.dumpThreadsOnFailure: false" + NL
                + "}";
    }

    /** Node bootstrap configuration pattern with client SSL enabled. */
    public static final String CLIENT_CONNECTOR_SSL_BOOTSTRAP_CONFIG = clientConnectorSslBootstrapConfig(null);

    /**
     *  Node bootstrap configuration pattern with client SSL enabled.
     *
     * @param ciphers Custom ciphers suites.
     * @return Config pattern.
     */
    public static String clientConnectorSslBootstrapConfig(@Nullable String ciphers) {
        return "ignite {" + NL
                + "  network: {" + NL
                + "    port: {}," + NL
                + "    nodeFinder: {" + NL
                + "      netClusterNodes: [ {} ]" + NL
                + "    }," + NL
                + "  }," + NL
                + "  clientConnector: {" + NL
                + "    port: {}," + NL
                + "    ssl: {" + NL
                + "      enabled: true," + NL
                + "      clientAuth: require," + NL
                + "      keyStore: {" + NL
                + "        path: \"" + escapeWindowsPath(resolvedKeystorePath) + "\"," + NL
                + "        password: " + keyStorePassword + NL
                + "      }," + NL
                + "      trustStore: {" + NL
                + "        type: JKS," + NL
                + "        path: \"" + escapeWindowsPath(resolvedTruststorePath) + "\"," + NL
                + "        password: " + trustStorePassword + NL
                + "      }," + NL
                + (nullOrBlank(ciphers) ? "" : "      ciphers: \"" + ciphers + "\"")
                + "    }" + NL
                + "  }," + NL
                + "  rest: {" + NL
                + "    port: {}," + NL
                + "    ssl.port: {}" + NL
                + "  }," + NL
                + "  failureHandler.handler.type: noop," + NL
                + "  failureHandler.dumpThreadsOnFailure: false" + NL
                + "}";
    }
}
