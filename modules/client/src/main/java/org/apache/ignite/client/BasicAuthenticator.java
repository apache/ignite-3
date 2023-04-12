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

package org.apache.ignite.client;

import java.util.Map;

/**
 * Basic authenticator.
 */
public class BasicAuthenticator implements IgniteClientAuthenticator {
    private final String username;

    private final String password;

    private BasicAuthenticator(String username, String password) {
        this.username = username;
        this.password = password;
    }

    /**
     * Creates a new builder.
     *
     * @return Builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String type() {
        return "basic";
    }

    @Override
    public String user() {
        return username;
    }

    @Override
    public Object key() {
        return password;
    }

    static class Builder {
        private String username;
        private String password;

        public Builder username(String username) {
            this.username = username;

            return this;
        }

        public Builder password(String password) {
            this.password = password;

            return this;
        }

        public BasicAuthenticator build() {
            return new BasicAuthenticator(username, password);
        }
    }
}
