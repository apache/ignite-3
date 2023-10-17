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

package org.apache.ignite.internal.security.authentication;

import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.security.configuration.SecurityView;
import org.jetbrains.annotations.Nullable;

/** Stub of {@link ConfigurationNotificationEvent} for tests. */
public class StubSecurityViewEvent implements ConfigurationNotificationEvent<SecurityView> {
    private final SecurityView oldValue;
    private final SecurityView newValue;

    public StubSecurityViewEvent(SecurityView oldValue, SecurityView newValue) {
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

    @Override
    @Nullable
    public SecurityView oldValue() {
        return oldValue;
    }

    @Override
    public <T> @Nullable T oldValue(Class<T> viewClass) {
        return null;
    }

    @Override
    @Nullable
    public SecurityView newValue() {
        return newValue;
    }

    @Override
    public <T> @Nullable T newValue(Class<T> viewClass) {
        return null;
    }

    @Override
    public long storageRevision() {
        return 0;
    }

    @Override
    public @Nullable String oldName(Class<?> viewClass) {
        return null;
    }

    @Override
    public @Nullable String newName(Class<?> viewClass) {
        return null;
    }
}
