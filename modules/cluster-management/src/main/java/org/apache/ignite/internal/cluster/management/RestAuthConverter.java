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
import org.apache.ignite.internal.cluster.management.network.auth.AuthenticationProvider;
import org.apache.ignite.internal.cluster.management.network.auth.BasicAuthenticationProvider;
import org.apache.ignite.internal.cluster.management.network.auth.RestAuthentication;
import org.apache.ignite.rest.AuthenticationProviderConfig;
import org.apache.ignite.rest.AuthenticationType;
import org.apache.ignite.rest.BasicAuthenticationProviderConfig;
import org.apache.ignite.rest.RestAuthenticationConfig;

/** Converter for {@link RestAuthentication}. */
class RestAuthConverter {

    /**
     * Converts {@link RestAuthentication} to {@link RestAuthenticationConfig}.
     *
     * @param restAuthentication Rest auth.
     * @return Rest auth config.
     */
    static RestAuthenticationConfig toRestAuthenticationConfig(RestAuthentication restAuthentication) {
        List<AuthenticationProviderConfig> providerConfigs = restAuthentication.providers()
                .stream()
                .map(RestAuthConverter::toAuthenticationProviderConfig)
                .collect(Collectors.toList());

        return new RestAuthenticationConfig(restAuthentication.enabled(), providerConfigs);
    }

    private static AuthenticationProviderConfig toAuthenticationProviderConfig(AuthenticationProvider provider) {
        AuthenticationType type = AuthenticationType.parse(provider.type());
        if (type == AuthenticationType.BASIC) {
            BasicAuthenticationProvider basicAuthProvider = (BasicAuthenticationProvider) provider;
            return new BasicAuthenticationProviderConfig(basicAuthProvider.name(), basicAuthProvider.login(), basicAuthProvider.password());
        } else {
            throw new IllegalArgumentException("Unsupported authentication type: " + type);
        }
    }
}
