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

/** Builder of {@link RestNode}. */
public class RestNodeBuilder {
    private String keyStorePath = "ssl/keystore.p12";
    private String keyStorePassword = "changeit";
    private String trustStorePath = "ssl/truststore.jks";
    private String trustStorePassword = "changeit";
    private Path workDir;
    private String name;
    private int networkPort;
    private int httpPort;
    private int httpsPort;
    private boolean sslEnabled = false;

    private boolean sslClientAuthEnabled = false;
    private boolean dualProtocol = false;

    private String ciphers = "";

    public RestNodeBuilder keyStorePath(String keyStorePath) {
        this.keyStorePath = keyStorePath;
        return this;
    }

    public RestNodeBuilder keyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
        return this;
    }

    public RestNodeBuilder trustStorePath(String trustStorePath) {
        this.trustStorePath = trustStorePath;
        return this;
    }

    public RestNodeBuilder trustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
        return this;
    }

    public RestNodeBuilder workDir(Path workDir) {
        this.workDir = workDir;
        return this;
    }

    public RestNodeBuilder name(String name) {
        this.name = name;
        return this;
    }

    public RestNodeBuilder networkPort(int networkPort) {
        this.networkPort = networkPort;
        return this;
    }

    public RestNodeBuilder httpPort(int httpPort) {
        this.httpPort = httpPort;
        return this;
    }

    public RestNodeBuilder httpsPort(int httpsPort) {
        this.httpsPort = httpsPort;
        return this;
    }

    public RestNodeBuilder sslEnabled(boolean sslEnabled) {
        this.sslEnabled = sslEnabled;
        return this;
    }

    public RestNodeBuilder sslClientAuthEnabled(boolean sslClientAuthEnabled) {
        this.sslClientAuthEnabled = sslClientAuthEnabled;
        return this;
    }

    public RestNodeBuilder dualProtocol(boolean dualProtocol) {
        this.dualProtocol = dualProtocol;
        return this;
    }

    public RestNodeBuilder ciphers(String ciphers) {
        this.ciphers = ciphers;
        return this;
    }

    /** Builds {@link RestNode}. */
    public RestNode build() {
        return new RestNode(
                keyStorePath,
                keyStorePassword,
                trustStorePath,
                trustStorePassword,
                workDir,
                name,
                networkPort,
                httpPort,
                httpsPort,
                sslEnabled,
                sslClientAuthEnabled,
                dualProtocol,
                ciphers
        );
    }
}
