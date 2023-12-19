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

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.security.authentication.AuthenticationUtils;
import org.apache.ignite.internal.security.authentication.basic.BasicUserView;
import org.apache.ignite.internal.security.authentication.configuration.AuthenticationView;

/**
 * Event factory for user configuration changes. Fires events when basic users are added, removed or updated.
 */
public class UserEventFactory implements ConfigurationNamedListListener<BasicUserView> {
    private final Function<AuthenticationEventParameters, CompletableFuture<Void>> notifier;

    public UserEventFactory(Function<AuthenticationEventParameters, CompletableFuture<Void>> notifier) {
        this.notifier = notifier;
    }

    @Override
    public CompletableFuture<?> onRename(ConfigurationNotificationEvent<BasicUserView> ctx) {
        AuthenticationView authenticationView = ctx.oldValue(AuthenticationView.class);
        String basicProviderName = AuthenticationUtils.findBasicProviderName(authenticationView.providers());
        return notifier.apply(UserEventParameters.removed(basicProviderName, ctx.oldValue().username()));
    }

    @Override
    public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<BasicUserView> ctx) {
        AuthenticationView authenticationView = ctx.oldValue(AuthenticationView.class);
        String basicProviderName = AuthenticationUtils.findBasicProviderName(authenticationView.providers());
        return notifier.apply(UserEventParameters.removed(basicProviderName, ctx.oldValue().username()));
    }

    @Override
    public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<BasicUserView> ctx) {
        AuthenticationView authenticationView = ctx.oldValue(AuthenticationView.class);
        String basicProviderName = AuthenticationUtils.findBasicProviderName(authenticationView.providers());
        return notifier.apply(UserEventParameters.updated(basicProviderName, ctx.oldValue().username()));
    }
}
