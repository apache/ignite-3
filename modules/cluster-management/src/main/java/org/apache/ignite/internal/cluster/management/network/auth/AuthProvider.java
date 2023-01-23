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

import java.io.Serializable;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.rest.AuthProviderConfig;
import org.apache.ignite.rest.BasicAuthProviderConfig;

/** Represents authentication provider config. */
public interface AuthProvider extends NetworkMessage, Serializable {

    /** Type of the provider. */
    String type();

    /** Name of the provider. */
    String name();

    /**
     * Creates a new {@link AuthProvider} instance. Acts like a constructor replacement.
     *
     * @param msgFactory Message factory to instantiate builder.
     * @param authProviderConfig Authentication provider config.
     * @return AuthProvider instance.
     */
    static AuthProvider restAuthConfig(
            CmgMessagesFactory msgFactory,
            AuthProviderConfig authProviderConfig
    ) {
        switch (authProviderConfig.type()) {
            case BASIC:
                return BasicAuthProvider.basicAuthProvider(msgFactory, (BasicAuthProviderConfig) authProviderConfig);
            default:
                throw new IllegalArgumentException("Unsupported auth");
        }
    }
}
