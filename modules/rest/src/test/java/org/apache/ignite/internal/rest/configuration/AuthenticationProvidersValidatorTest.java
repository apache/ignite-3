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
import org.apache.ignite.internal.rest.configuration.stub.StubAuthenticationProviderListView;
import org.apache.ignite.internal.rest.configuration.stub.StubBasicAuthenticationProviderView;
import org.junit.jupiter.api.Test;

class AuthenticationProvidersValidatorTest {

    @Test
    public void basicProvider() {
        ValidationContext<NamedListView<AuthenticationProviderView>> ctx = mockValidationContext(
                new StubAuthenticationProviderListView(Collections.emptyList()),
                new StubAuthenticationProviderListView(
                        Collections.singletonList(new StubBasicAuthenticationProviderView("login", "password")))
        );
        validate(AuthenticationProvidersValidatorImpl.INSTANCE, mock(AuthenticationProvidersValidator.class), ctx, null);
    }

    @Test
    public void basicProviderWithNullLogin() {
        ValidationContext<NamedListView<AuthenticationProviderView>> ctx = mockValidationContext(
                new StubAuthenticationProviderListView(Collections.emptyList()),
                new StubAuthenticationProviderListView(Collections.singletonList(new StubBasicAuthenticationProviderView(null, "password")))
        );
        validate(AuthenticationProvidersValidatorImpl.INSTANCE, mock(AuthenticationProvidersValidator.class), ctx,
                "Login must not be blank");
    }

    @Test
    public void basicProviderWithEmptyLogin() {
        ValidationContext<NamedListView<AuthenticationProviderView>> ctx = mockValidationContext(
                new StubAuthenticationProviderListView(Collections.emptyList()),
                new StubAuthenticationProviderListView(Collections.singletonList(new StubBasicAuthenticationProviderView("", "password")))
        );
        validate(AuthenticationProvidersValidatorImpl.INSTANCE, mock(AuthenticationProvidersValidator.class), ctx,
                "Login must not be blank");
    }

    @Test
    public void basicProviderWithNullPassword() {
        ValidationContext<NamedListView<AuthenticationProviderView>> ctx = mockValidationContext(
                new StubAuthenticationProviderListView(Collections.emptyList()),
                new StubAuthenticationProviderListView(Collections.singletonList(new StubBasicAuthenticationProviderView("login", null)))
        );
        validate(AuthenticationProvidersValidatorImpl.INSTANCE, mock(AuthenticationProvidersValidator.class), ctx,
                "Password must not be blank");
    }

    @Test
    public void basicProviderWithEmptyPassword() {
        ValidationContext<NamedListView<AuthenticationProviderView>> ctx = mockValidationContext(
                new StubAuthenticationProviderListView(Collections.emptyList()),
                new StubAuthenticationProviderListView(Collections.singletonList(new StubBasicAuthenticationProviderView("login", "")))
        );
        validate(AuthenticationProvidersValidatorImpl.INSTANCE, mock(AuthenticationProvidersValidator.class), ctx,
                "Password must not be blank");
    }
}
