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

package org.apache.ignite.internal.rest.auth;

import java.util.Objects;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.rest.configuration.AuthProviderView;
import org.apache.ignite.internal.rest.configuration.BasicAuthProviderView;
import org.apache.ignite.rest.AuthType;
import org.jetbrains.annotations.Nullable;

/** Factory for {@link Authenticator}. */
class AuthenticatorFactory {

    private static final IgniteLogger LOG = Loggers.forClass(AuthenticatorFactory.class);

    @Nullable
    static Authenticator create(AuthProviderView view) {
        AuthType authType = AuthType.parse(view.type());
        if (authType != null) {
            return create(authType, view);
        } else {
            LOG.error("Unknown auth type: " + view);
            return null;
        }
    }

    @Nullable
    private static Authenticator create(AuthType type, AuthProviderView view) {
        if (Objects.requireNonNull(type) == AuthType.BASIC) {
            BasicAuthProviderView basicAuthProviderView = (BasicAuthProviderView) view;
            return new BasicAuthenticator(basicAuthProviderView.login(), basicAuthProviderView.password());
        } else {
            LOG.error("Couldn't create authenticator: " + view);
            return null;
        }
    }
}
