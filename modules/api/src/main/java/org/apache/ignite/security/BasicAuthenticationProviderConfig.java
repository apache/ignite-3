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

package org.apache.ignite.security;

import org.apache.ignite.internal.util.StringUtils;

/** Configuration of basic authentication provider. */
public class BasicAuthenticationProviderConfig implements AuthenticationProviderConfig {

    private final String name;

    private final String login;

    private final String password;

    /** Constructor. */
    public BasicAuthenticationProviderConfig(String name, String login, String password) {
        if (StringUtils.nullOrEmpty(name)) {
            throw new IllegalArgumentException("name cannot be null or empty");
        }

        if (StringUtils.nullOrEmpty(login)) {
            throw new IllegalArgumentException("login cannot be null or empty");
        }

        if (StringUtils.nullOrEmpty(password)) {
            throw new IllegalArgumentException("password cannot be null or empty");
        }

        this.name = name;
        this.login = login;
        this.password = password;
    }

    public String login() {
        return login;
    }

    public String password() {
        return password;
    }

    @Override
    public AuthenticationType type() {
        return AuthenticationType.BASIC;
    }

    @Override
    public String name() {
        return name;
    }
}
