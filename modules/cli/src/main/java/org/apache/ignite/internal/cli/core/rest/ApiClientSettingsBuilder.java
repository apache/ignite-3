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

package org.apache.ignite.internal.cli.core.rest;

/** Builder for {@link ApiClientSettings}. */
public class ApiClientSettingsBuilder {
    private String basePath;
    private String keyStorePath;
    private String keyStorePassword;
    private String trustStorePath;
    private String trustStorePassword;
    private String ciphers;
    private String basicAuthenticationUsername;
    private String basicAuthenticationPassword;

    public ApiClientSettingsBuilder basePath(String basePath) {
        this.basePath = basePath;
        return this;
    }

    public ApiClientSettingsBuilder keyStorePath(String keyStorePath) {
        this.keyStorePath = keyStorePath;
        return this;
    }

    public ApiClientSettingsBuilder keyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
        return this;
    }

    public ApiClientSettingsBuilder trustStorePath(String trustStorePath) {
        this.trustStorePath = trustStorePath;
        return this;
    }

    public ApiClientSettingsBuilder trustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
        return this;
    }

    public ApiClientSettingsBuilder ciphers(String ciphers) {
        this.ciphers = ciphers;
        return this;
    }

    public ApiClientSettingsBuilder basicAuthenticationUsername(String basicAuthenticationUsername) {
        this.basicAuthenticationUsername = basicAuthenticationUsername;
        return this;
    }

    public ApiClientSettingsBuilder basicAuthenticationPassword(String basicAuthenticationPassword) {
        this.basicAuthenticationPassword = basicAuthenticationPassword;
        return this;
    }

    public ApiClientSettings build() {
        return new ApiClientSettings(basePath, keyStorePath, keyStorePassword, trustStorePath, trustStorePassword,
                ciphers, basicAuthenticationUsername, basicAuthenticationPassword);
    }
}
