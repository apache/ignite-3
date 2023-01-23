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

package org.apache.ignite.internal.rest.configuration;

import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.mockValidationContext;
import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.validate;
import static org.mockito.Mockito.mock;

import java.util.Collections;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.internal.rest.configuration.stub.StubAuthProviderListView;
import org.apache.ignite.internal.rest.configuration.stub.StubBasicAuthProviderView;
import org.junit.jupiter.api.Test;

class AuthProvidersValidatorTest {

    @Test
    public void basicProvider() {
        ValidationContext<NamedListView<AuthProviderView>> ctx = mockValidationContext(
                new StubAuthProviderListView(Collections.emptyList()),
                new StubAuthProviderListView(Collections.singletonList(new StubBasicAuthProviderView("login", "password")))
        );
        validate(AuthProvidersValidatorImpl.INSTANCE, mock(AuthProvidersValidator.class), ctx, null);
    }

    @Test
    public void basicProviderWithNullLogin() {
        ValidationContext<NamedListView<AuthProviderView>> ctx = mockValidationContext(
                new StubAuthProviderListView(Collections.emptyList()),
                new StubAuthProviderListView(Collections.singletonList(new StubBasicAuthProviderView(null, "password")))
        );
        validate(AuthProvidersValidatorImpl.INSTANCE, mock(AuthProvidersValidator.class), ctx, "Login must not be blank");
    }

    @Test
    public void basicProviderWithEmptyLogin() {
        ValidationContext<NamedListView<AuthProviderView>> ctx = mockValidationContext(
                new StubAuthProviderListView(Collections.emptyList()),
                new StubAuthProviderListView(Collections.singletonList(new StubBasicAuthProviderView("", "password")))
        );
        validate(AuthProvidersValidatorImpl.INSTANCE, mock(AuthProvidersValidator.class), ctx, "Login must not be blank");
    }

    @Test
    public void basicProviderWithNullPassword() {
        ValidationContext<NamedListView<AuthProviderView>> ctx = mockValidationContext(
                new StubAuthProviderListView(Collections.emptyList()),
                new StubAuthProviderListView(Collections.singletonList(new StubBasicAuthProviderView("login", null)))
        );
        validate(AuthProvidersValidatorImpl.INSTANCE, mock(AuthProvidersValidator.class), ctx, "Password must not be blank");
    }

    @Test
    public void basicProviderWithEmptyPassword() {
        ValidationContext<NamedListView<AuthProviderView>> ctx = mockValidationContext(
                new StubAuthProviderListView(Collections.emptyList()),
                new StubAuthProviderListView(Collections.singletonList(new StubBasicAuthProviderView("login", "")))
        );
        validate(AuthProvidersValidatorImpl.INSTANCE, mock(AuthProvidersValidator.class), ctx, "Password must not be blank");
    }
}
