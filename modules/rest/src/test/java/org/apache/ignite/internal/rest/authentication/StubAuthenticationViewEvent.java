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

package org.apache.ignite.internal.rest.authentication;

import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.configuration.AuthenticationView;
import org.jetbrains.annotations.Nullable;

/** Stub of {@link ConfigurationNotificationEvent} for tests. */
public class StubAuthenticationViewEvent implements ConfigurationNotificationEvent<AuthenticationView> {
    private final AuthenticationView oldValue;
    private final AuthenticationView newValue;

    public StubAuthenticationViewEvent(AuthenticationView oldValue, AuthenticationView newValue) {
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

    @Override
    @Nullable
    public AuthenticationView oldValue() {
        return oldValue;
    }

    @Override
    @Nullable
    public AuthenticationView newValue() {
        return newValue;
    }

    @Override
    public long storageRevision() {
        return 0;
    }

    @Override
    @Nullable
    public <T extends ConfigurationProperty> T config(Class<T> configClass) {
        return null;
    }

    @Override
    @Nullable
    public String name(Class<? extends ConfigurationProperty> configClass) {
        return null;
    }
}
