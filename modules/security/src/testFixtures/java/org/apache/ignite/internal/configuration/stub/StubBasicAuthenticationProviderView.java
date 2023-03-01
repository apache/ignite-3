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

package org.apache.ignite.internal.configuration.stub;

import java.util.UUID;
import org.apache.ignite.internal.configuration.BasicAuthenticationProviderView;


/** Stub of {@link BasicAuthenticationProviderView} for tests. */
public class StubBasicAuthenticationProviderView implements BasicAuthenticationProviderView {

    private final String type = "basic";

    private final String name;

    private final String login;

    private final String password;

    /** Constructor. */
    public StubBasicAuthenticationProviderView(String login, String password) {
        this.name = UUID.randomUUID().toString();
        this.login = login;
        this.password = password;
    }

    /** Constructor. */
    public StubBasicAuthenticationProviderView(String name, String login, String password) {
        this.name = name;
        this.login = login;
        this.password = password;
    }

    @Override
    public String type() {
        return type;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String login() {
        return login;
    }

    @Override
    public String password() {
        return password;
    }
}
