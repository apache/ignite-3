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

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;

/**
 * Event factory for authentication switch changes. Fires events when authentication is enabled or disabled.
 */
public class SecurityEnabledDisabledEventFactory implements ConfigurationListener<Boolean> {
    private final Function<AuthenticationEventParameters, CompletableFuture<Void>> notifier;

    public SecurityEnabledDisabledEventFactory(Function<AuthenticationEventParameters, CompletableFuture<Void>> notifier) {
        this.notifier = notifier;
    }

    @Override
    public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<Boolean> ctx) {
        Boolean oldValue = ctx.oldValue();
        Boolean newValue = ctx.newValue();

        if (oldValue == null || !oldValue.equals(newValue)) {
            return notifier.apply(AuthenticationSwitchedParameters.enabled(newValue));
        } else {
            return nullCompletedFuture();
        }
    }
}
