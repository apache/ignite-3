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
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessageGroup.Commands;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessagesFactory;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.annotations.Transferable;
import org.apache.ignite.rest.RestAuthConfig;


/** Represents authentication config. */
@Transferable(Commands.REST_AUTH)
public interface RestAuth extends NetworkMessage, Serializable {

    /** If Security is enabled. */
    boolean enabled();

    /** List of authentication providers. */
    List<AuthProvider> providers();

    /**
     * Creates a new {@link RestAuth} instance. Acts like a constructor replacement.
     *
     * @param msgFactory Message factory to instantiate builder.
     * @param restAuthConfig Authentication  config.
     * @return RestAuth instance.
     */
    static RestAuth restAuth(
            CmgMessagesFactory msgFactory,
            RestAuthConfig restAuthConfig
    ) {
        List<AuthProvider> providers = restAuthConfig.providers().stream()
                .map(it -> AuthProvider.restAuthConfig(msgFactory, it))
                .collect(Collectors.toList());
        return msgFactory.restAuth()
                .enabled(restAuthConfig.enabled())
                .providers(providers)
                .build();
    }
}
