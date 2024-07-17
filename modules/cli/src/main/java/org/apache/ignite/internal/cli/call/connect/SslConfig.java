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

package org.apache.ignite.internal.cli.call.connect;

/** SSL configuration for creating HTTP client. */
public class SslConfig {
    private String keyStorePath;
    private String keyStorePassword;
    private String trustStorePath;
    private String trustStorePassword;

    public String keyStorePath() {
        return keyStorePath;
    }

    public void keyStorePath(String keyStorePath) {
        this.keyStorePath = keyStorePath;
    }

    public String keyStorePassword() {
        return keyStorePassword;
    }

    public void keyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
    }

    public String trustStorePath() {
        return trustStorePath;
    }

    public void trustStorePath(String trustStorePath) {
        this.trustStorePath = trustStorePath;
    }

    public String trustStorePassword() {
        return trustStorePassword;
    }

    public void trustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
    }
}
