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

package org.apache.ignite;

import org.apache.ignite.client.SslConfiguration;

/**
 * Describes properties for SSL configuration.
 */
public class SslConfigurationProperties implements SslConfiguration {
    private Boolean enabled;
    private Iterable<String> ciphers;
    private String keyStorePath;
    private String keyStorePassword;
    private String trustStorePath;
    private String trustStorePassword;

    @Override
    public boolean enabled() {
        return enabled;
    }

    @Override
    public Iterable<String> ciphers() {
        return ciphers;
    }

    @Override
    public String keyStorePath() {
        return keyStorePath;
    }

    @Override
    public String keyStorePassword() {
        return keyStorePassword;
    }

    @Override
    public String trustStorePath() {
        return trustStorePath;
    }

    @Override
    public String trustStorePassword() {
        return trustStorePassword;
    }

    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }

    public void setCiphers(Iterable<String> ciphers) {
        this.ciphers = ciphers;
    }

    public void setKeyStorePath(String keyStorePath) {
        this.keyStorePath = keyStorePath;
    }

    public void setKeyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
    }

    public void setTrustStorePath(String trustStorePath) {
        this.trustStorePath = trustStorePath;
    }

    public void setTrustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
    }
}
