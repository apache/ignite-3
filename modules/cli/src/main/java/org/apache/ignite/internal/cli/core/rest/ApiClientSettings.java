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

import java.util.Objects;

/** Api client settings. */
public class ApiClientSettings {

    private final String basePath;

    private final String keyStorePath;

    private final String keyStorePassword;

    private final String trustStorePath;

    private final String trustStorePassword;

    private final String ciphers;

    private final String basicAuthenticationUsername;

    private final String basicAuthenticationPassword;

    ApiClientSettings(String basePath, String keyStorePath, String keyStorePassword, String trustStorePath, String trustStorePassword,
            String ciphers, String basicAuthenticationUsername, String basicAuthenticationPassword) {
        this.basePath = basePath;
        this.keyStorePath = keyStorePath;
        this.keyStorePassword = keyStorePassword;
        this.trustStorePath = trustStorePath;
        this.trustStorePassword = trustStorePassword;
        this.ciphers = ciphers;
        this.basicAuthenticationUsername = basicAuthenticationUsername;
        this.basicAuthenticationPassword = basicAuthenticationPassword;
    }

    public static ApiClientSettingsBuilder builder() {
        return new ApiClientSettingsBuilder();
    }

    public String basePath() {
        return basePath;
    }

    public String keyStorePath() {
        return keyStorePath;
    }

    public String keyStorePassword() {
        return keyStorePassword;
    }

    public String trustStorePath() {
        return trustStorePath;
    }

    public String trustStorePassword() {
        return trustStorePassword;
    }

    public String ciphers() {
        return ciphers;
    }

    public String basicAuthenticationUsername() {
        return basicAuthenticationUsername;
    }

    public String basicAuthenticationPassword() {
        return basicAuthenticationPassword;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ApiClientSettings that = (ApiClientSettings) o;
        return Objects.equals(basePath, that.basePath) && Objects.equals(keyStorePath, that.keyStorePath)
                && Objects.equals(keyStorePassword, that.keyStorePassword) && Objects.equals(trustStorePath, that.trustStorePath)
                && Objects.equals(trustStorePassword, that.trustStorePassword)
                && Objects.equals(ciphers, that.ciphers)
                && Objects.equals(basicAuthenticationUsername, that.basicAuthenticationUsername)
                && Objects.equals(basicAuthenticationPassword, that.basicAuthenticationPassword);
    }

    @Override
    public int hashCode() {
        return Objects.hash(basePath, keyStorePath, keyStorePassword, trustStorePath, trustStorePassword,
                ciphers, basicAuthenticationUsername, basicAuthenticationPassword);
    }
}
