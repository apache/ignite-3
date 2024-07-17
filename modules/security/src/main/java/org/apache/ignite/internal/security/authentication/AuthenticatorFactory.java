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

package org.apache.ignite.internal.security.authentication;

import java.util.stream.Collectors;
import org.apache.ignite.internal.security.authentication.basic.BasicAuthenticationProviderView;
import org.apache.ignite.internal.security.authentication.basic.BasicAuthenticator;
import org.apache.ignite.internal.security.authentication.basic.BasicUser;
import org.apache.ignite.internal.security.authentication.configuration.AuthenticationProviderView;

/** Factory for {@link Authenticator}. */
class AuthenticatorFactory {
    static Authenticator create(AuthenticationProviderView view) {
        if (view instanceof BasicAuthenticationProviderView) {
            BasicAuthenticationProviderView basicAuthProviderView = (BasicAuthenticationProviderView) view;
            return new BasicAuthenticator(
                    view.name(),
                    basicAuthProviderView.users()
                            .stream()
                            .map(basicUserView -> new BasicUser(basicUserView.username(), basicUserView.password()))
                            .collect(Collectors.toList())
            );
        } else {
            throw new IllegalArgumentException("Unexpected authentication provider view: " + view);
        }
    }
}
