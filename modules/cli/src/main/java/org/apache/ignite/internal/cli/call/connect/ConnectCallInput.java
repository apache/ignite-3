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

import org.apache.ignite.internal.cli.core.call.CallInput;
import org.jetbrains.annotations.Nullable;

/** Input for the {@link ConnectCall} call. */
public class ConnectCallInput implements CallInput {

    private final String url;

    @Nullable
    private final String username;

    @Nullable
    private final String password;

    private ConnectCallInput(String url, @Nullable String username, @Nullable String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    public String url() {
        return url;
    }

    /**
     * Provided username.
     *
     * @return username
     */
    @Nullable
    public String username() {
        return username;
    }

    /**
     * Provided password.
     *
     * @return password
     */
    @Nullable
    public String password() {
        return password;
    }

    /**
     * Builder method provider.
     *
     * @return new instance of {@link ConnectCallInputBuilder}.
     */
    public static ConnectCallInputBuilder builder() {
        return new ConnectCallInputBuilder();
    }

    /** Builder for {@link ConnectCallInput}. */
    public static class ConnectCallInputBuilder {

        private String url;
        @Nullable
        private String username;

        @Nullable
        private String password;

        private ConnectCallInputBuilder() {
        }

        public ConnectCallInputBuilder url(String url) {
            this.url = url;
            return this;
        }

        public ConnectCallInputBuilder username(@Nullable String username) {
            this.username = username;
            return this;
        }

        public ConnectCallInputBuilder password(@Nullable String password) {
            this.password = password;
            return this;
        }

        public ConnectCallInput build() {
            return new ConnectCallInput(url, username, password);
        }
    }
}
