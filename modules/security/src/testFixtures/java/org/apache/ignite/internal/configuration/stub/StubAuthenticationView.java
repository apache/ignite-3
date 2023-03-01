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

package org.apache.ignite.internal.configuration.stub;

import java.util.Collections;
import java.util.List;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.internal.configuration.AuthenticationProviderView;
import org.apache.ignite.internal.configuration.AuthenticationView;

/** Stub of {@link AuthenticationView} for tests. */
public class StubAuthenticationView implements AuthenticationView {

    private final boolean enabled;
    private final List<AuthenticationProviderView> providers;

    public StubAuthenticationView(boolean enabled, AuthenticationProviderView provider) {
        this(enabled, Collections.singletonList(provider));
    }

    public StubAuthenticationView(boolean enabled, List<AuthenticationProviderView> providers) {
        this.enabled = enabled;
        this.providers = providers;
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    @Override
    public NamedListView<AuthenticationProviderView> providers() {
        return new StubAuthenticationProviderListView(providers);
    }
}
