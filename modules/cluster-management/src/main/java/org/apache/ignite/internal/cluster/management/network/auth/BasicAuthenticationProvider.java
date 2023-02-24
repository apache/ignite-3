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

package org.apache.ignite.internal.cluster.management.network.auth;

import org.apache.ignite.internal.cluster.management.network.messages.CmgMessageGroup.Commands;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.network.annotations.Transferable;
import org.apache.ignite.rest.BasicAuthenticationProviderConfig;

/** Represents basic authentication provider config. */
@Transferable(Commands.BASIC_AUTH_PROVIDER)
public interface BasicAuthenticationProvider extends AuthenticationProvider {

    /** Login. */
    String login();

    /** Password. */
    String password();

    /**
     * Creates a new {@link BasicAuthenticationProvider} instance. Acts like a constructor replacement.
     *
     * @param msgFactory Message factory to instantiate builder.
     * @param basicAuthenticationProviderConfig Basic authentication provider config.
     * @return BasicAuthProvider instance.
     */
    static BasicAuthenticationProvider basicAuthenticationProvider(
            CmgMessagesFactory msgFactory,
            BasicAuthenticationProviderConfig basicAuthenticationProviderConfig
    ) {
        return msgFactory.basicAuthenticationProvider()
                .name(basicAuthenticationProviderConfig.name())
                .login(basicAuthenticationProviderConfig.login())
                .password(basicAuthenticationProviderConfig.password())
                .type(basicAuthenticationProviderConfig.type().toString())
                .build();
    }
}
