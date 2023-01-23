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

package org.apache.ignite.internal.cluster.management;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cluster.management.network.auth.AuthProvider;
import org.apache.ignite.internal.cluster.management.network.auth.BasicAuthProvider;
import org.apache.ignite.internal.cluster.management.network.auth.RestAuth;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.rest.AuthProviderConfig;
import org.apache.ignite.rest.AuthType;
import org.apache.ignite.rest.BasicAuthProviderConfig;
import org.apache.ignite.rest.RestAuthConfig;
import org.jetbrains.annotations.Nullable;

/** Converter for {@link RestAuth}. */
public class RestAuthConverter {

    private static final IgniteLogger LOG = Loggers.forClass(DistributedConfigurationUpdater.class);

    /**
     * Converts {@link RestAuth} to {@link RestAuthConfig}.
     *
     * @param restAuth Rest auth.
     * @return Rest auth config.
     */
    public static RestAuthConfig toRestAuthConfig(RestAuth restAuth) {
        List<AuthProviderConfig> providerConfigs = restAuth.providers()
                .stream()
                .map(RestAuthConverter::toAuthProviderConfig)
                .collect(Collectors.toList());

        return new RestAuthConfig(restAuth.enabled(), providerConfigs);
    }

    @Nullable
    private static AuthProviderConfig toAuthProviderConfig(AuthProvider authProvider) {
        AuthType type = AuthType.parse(authProvider.type());
        if (type == AuthType.BASIC) {
            BasicAuthProvider basicAuthProvider = (BasicAuthProvider) authProvider;
            return new BasicAuthProviderConfig(basicAuthProvider.name(), basicAuthProvider.login(), basicAuthProvider.password());
        } else {
            LOG.error("Unsupported authentication type: " + authProvider.type());
            return null;
        }
    }
}
