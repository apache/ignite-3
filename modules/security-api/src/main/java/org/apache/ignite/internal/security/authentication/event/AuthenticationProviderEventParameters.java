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

package org.apache.ignite.internal.security.authentication.event;

import static org.apache.ignite.internal.security.authentication.event.AuthenticationEvent.AUTHENTICATION_PROVIDER_REMOVED;
import static org.apache.ignite.internal.security.authentication.event.AuthenticationEvent.AUTHENTICATION_PROVIDER_UPDATED;

/**
 * Represents the authentication provider event.
 */
public class AuthenticationProviderEventParameters implements AuthenticationEventParameters {
    private final AuthenticationEvent type;

    private final String providerName;

    private AuthenticationProviderEventParameters(AuthenticationEvent type, String providerName) {
        this.type = type;
        this.providerName = providerName;
    }

    public static AuthenticationProviderEventParameters updated(String name) {
        return new AuthenticationProviderEventParameters(AUTHENTICATION_PROVIDER_UPDATED, name);
    }

    public static AuthenticationProviderEventParameters removed(String name) {
        return new AuthenticationProviderEventParameters(AUTHENTICATION_PROVIDER_REMOVED, name);
    }

    @Override
    public AuthenticationEvent type() {
        return type;
    }

    public String name() {
        return providerName;
    }
}
