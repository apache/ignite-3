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

package org.apache.ignite.internal.rest.configuration.stub;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.internal.rest.configuration.AuthenticationProviderView;
import org.jetbrains.annotations.Nullable;

/** Stub of {@link NamedListView} for tests. */
public class StubAuthenticationProviderListView implements NamedListView<AuthenticationProviderView> {

    private final List<AuthenticationProviderView> providers;

    public StubAuthenticationProviderListView(List<AuthenticationProviderView> providers) {
        this.providers = List.copyOf(providers);
    }

    @Override
    public List<String> namedListKeys() {
        return providers.stream()
                .map(AuthenticationProviderView::name)
                .collect(Collectors.toList());
    }

    @Override
    @Nullable
    public AuthenticationProviderView get(String key) {
        return providers.stream()
                .filter(it -> key.equals(it.name()))
                .findFirst()
                .orElse(null);
    }

    @Override
    public AuthenticationProviderView get(int index) throws IndexOutOfBoundsException {
        return providers.get(index);
    }

    @Override
    public int size() {
        return providers.size();
    }
}
