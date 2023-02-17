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

package org.apache.ignite.client.handler;

/** Test SSL configuration. It is needed for grouping parameters. */
public class TestSslConfig {
    private final String algorithm;

    private final String keyStorePassword;

    private final String keyStorePath;

    private TestSslConfig(String algorithm, String keyStorePassword, String keyStorePath) {
        this.algorithm = algorithm;
        this.keyStorePassword = keyStorePassword;
        this.keyStorePath = keyStorePath;
    }

    public String algorithm() {
        return algorithm;
    }

    public String keyStorePassword() {
        return keyStorePassword;
    }

    public String keyStorePath() {
        return keyStorePath;
    }

    static TestSslConfigBuilder builder() {
        return new TestSslConfigBuilder();
    }

    static class TestSslConfigBuilder {
        private String algorithm;

        private String keyStorePassword;

        private String keyStorePath;

        public TestSslConfigBuilder algorithm(String algorithm) {
            this.algorithm = algorithm;
            return this;
        }

        public TestSslConfigBuilder keyStorePassword(String keyStorePassword) {
            this.keyStorePassword = keyStorePassword;
            return this;
        }

        public TestSslConfigBuilder keyStorePath(String keyStorePath) {
            this.keyStorePath = keyStorePath;
            return this;
        }

        public TestSslConfig build() {
            return new TestSslConfig(algorithm, keyStorePassword, keyStorePath);
        }
    }
}
