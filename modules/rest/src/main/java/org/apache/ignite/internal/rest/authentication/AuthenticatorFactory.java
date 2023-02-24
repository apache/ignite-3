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

package org.apache.ignite.internal.rest.authentication;

import org.apache.ignite.internal.rest.configuration.AuthenticationProviderView;
import org.apache.ignite.internal.rest.configuration.BasicAuthenticationProviderView;
import org.apache.ignite.rest.AuthenticationType;

/** Factory for {@link Authenticator}. */
class AuthenticatorFactory {

    static Authenticator create(AuthenticationProviderView view) {
        AuthenticationType authenticationType = AuthenticationType.parse(view.type());
        if (authenticationType != null) {
            return create(authenticationType, view);
        } else {
            throw new IllegalArgumentException("Unknown auth type: " + view.type());
        }
    }

    private static Authenticator create(AuthenticationType type, AuthenticationProviderView view) {
        if (type == AuthenticationType.BASIC) {
            BasicAuthenticationProviderView basicAuthProviderView = (BasicAuthenticationProviderView) view;
            return new BasicAuthenticator(basicAuthProviderView.login(), basicAuthProviderView.password());
        } else {
            throw new IllegalArgumentException("Unexpected auth type: " + type);
        }
    }
}
