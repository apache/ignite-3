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

import static org.apache.ignite.internal.security.authentication.configuration.AuthenticationProviderConfigurationSchema.TYPE_BASIC;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.security.authentication.basic.BasicAuthenticationProviderConfiguration;
import org.apache.ignite.internal.security.authentication.configuration.AuthenticationProviderConfiguration;
import org.apache.ignite.internal.security.authentication.configuration.AuthenticationProviderView;
import org.apache.ignite.internal.security.configuration.SecurityConfiguration;

/**
 * Event factory for authentication provider configuration changes. Fires events when authentication providers are added, removed or
 * updated.
 */
public class AuthenticationProviderEventFactory implements ConfigurationNamedListListener<AuthenticationProviderView> {
    private final SecurityConfiguration securityConfiguration;

    private final UserEventFactory userEventFactory;

    private final Function<AuthenticationEventParameters, CompletableFuture<Void>> notifier;

    /**
     * Constructor.
     *
     * @param securityConfiguration Security configuration.
     * @param userEventFactory User event factory.
     * @param notifier Notifier.
     */
    public AuthenticationProviderEventFactory(
            SecurityConfiguration securityConfiguration,
            UserEventFactory userEventFactory,
            Function<AuthenticationEventParameters, CompletableFuture<Void>> notifier
    ) {
        this.securityConfiguration = securityConfiguration;
        this.userEventFactory = userEventFactory;
        this.notifier = notifier;
    }

    @Override
    public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<AuthenticationProviderView> ctx) {
        onCreate(ctx.newValue());
        return nullCompletedFuture();
    }

    private void onCreate(AuthenticationProviderView providerView) {
        if (TYPE_BASIC.equals(providerView.type())) {
            AuthenticationProviderConfiguration configuration = securityConfiguration.authentication()
                    .providers()
                    .get(providerView.name());
            if (configuration != null) {
                BasicAuthenticationProviderConfiguration basicCfg = (BasicAuthenticationProviderConfiguration) configuration;
                basicCfg.users().listenElements(userEventFactory);
            }
        }
    }

    @Override
    public CompletableFuture<?> onRename(ConfigurationNotificationEvent<AuthenticationProviderView> ctx) {
        onCreate(ctx.newValue());
        return notifier.apply(AuthenticationProviderEventParameters.removed(ctx.oldValue().name()));
    }

    @Override
    public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<AuthenticationProviderView> ctx) {
        return notifier.apply(AuthenticationProviderEventParameters.removed(ctx.oldValue().name()));
    }

    @Override
    public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<AuthenticationProviderView> ctx) {
        if (TYPE_BASIC.equals(ctx.oldValue().type()) && ctx.oldValue().type().equals(ctx.newValue().type())) {
            return nullCompletedFuture();
        } else {
            return notifier.apply(AuthenticationProviderEventParameters.updated(ctx.newValue().name()));
        }
    }
}
