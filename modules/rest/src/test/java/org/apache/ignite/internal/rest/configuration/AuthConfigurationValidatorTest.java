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
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.internal.rest.configuration.stub.StubAuthView;
import org.apache.ignite.internal.rest.configuration.stub.StubBasicAuthProviderView;
import org.junit.jupiter.api.Test;

class AuthConfigurationValidatorTest {

    @Test
    public void nullAuth() {
        ValidationContext<AuthView> ctx = mockValidationContext(
                new StubAuthView(false, Collections.emptyList()),
                null
        );
        validate(AuthConfigurationValidatorImpl.INSTANCE, mock(AuthConfigurationValidator.class), ctx,
                "Auth config must not be null");
    }

    @Test
    public void enableAuthEmptyProviders() {
        ValidationContext<AuthView> ctx = mockValidationContext(
                new StubAuthView(false, Collections.emptyList()),
                new StubAuthView(true, Collections.emptyList())
        );
        validate(AuthConfigurationValidatorImpl.INSTANCE, mock(AuthConfigurationValidator.class), ctx,
                "Providers must be present, if auth is enabled");
    }

    @Test
    public void enableAuthNotEmptyProviders() {
        ValidationContext<AuthView> ctx = mockValidationContext(
                new StubAuthView(false, Collections.emptyList()),
                new StubAuthView(true, new StubBasicAuthProviderView("basic", "admin", "admin"))
        );
        validate(AuthConfigurationValidatorImpl.INSTANCE, mock(AuthConfigurationValidator.class), ctx, null);
    }

    @Test
    public void disableAuthEmptyProviders() {
        ValidationContext<AuthView> ctx = mockValidationContext(
                new StubAuthView(true, new StubBasicAuthProviderView("basic", "admin", "admin")),
                new StubAuthView(false, Collections.emptyList())
        );
        validate(AuthConfigurationValidatorImpl.INSTANCE, mock(AuthConfigurationValidator.class), ctx, null);
    }

    @Test
    public void disableAuthNotEmptyProviders() {
        ValidationContext<AuthView> ctx = mockValidationContext(
                new StubAuthView(true, new StubBasicAuthProviderView("basic", "admin", "admin")),
                new StubAuthView(false, new StubBasicAuthProviderView("basic", "admin", "admin"))
        );
        validate(AuthConfigurationValidatorImpl.INSTANCE, mock(AuthConfigurationValidator.class), ctx, null);
    }
}
