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

import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.mockValidationContext;
import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.validate;
import static org.apache.ignite.internal.security.authentication.SecurityConfigurationModule.DEFAULT_PROVIDER_NAME;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.security.authentication.configuration.AuthenticationProviderView;
import org.apache.ignite.internal.security.authentication.configuration.validator.AuthenticationProvidersValidator;
import org.apache.ignite.internal.security.authentication.validator.AuthenticationProvidersValidatorImpl;
import org.apache.ignite.internal.security.configuration.SecurityChange;
import org.apache.ignite.internal.security.configuration.SecurityConfiguration;
import org.apache.ignite.internal.security.configuration.SecurityView;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ConfigurationExtension.class)
class AuthenticationProvidersValidatorImplTest extends BaseIgniteAbstractTest {
    @InjectConfiguration
    private SecurityConfiguration securityConfiguration;

    @Test
    public void enableAuthEmptyProviders() {
        // when
        SecurityView newValue = mutateConfiguration(securityConfiguration, change -> {
            change.changeEnabled(true);
        }).value();

        ValidationContext<NamedListView<? extends AuthenticationProviderView>> ctx = mockValidationContext(
                null,
                newValue.authentication().providers()
        );

        doReturn(newValue).when(ctx).getNewRoot(SecurityConfiguration.KEY);

        // then
        validate(
                AuthenticationProvidersValidatorImpl.INSTANCE,
                mock(AuthenticationProvidersValidator.class),
                ctx,
                "Default provider " + DEFAULT_PROVIDER_NAME + " is not removable"
        );
    }

    private static SecurityConfiguration mutateConfiguration(SecurityConfiguration configuration,
            Consumer<SecurityChange> consumer) {
        CompletableFuture<SecurityConfiguration> future = configuration.change(consumer)
                .thenApply(unused -> configuration);
        return future.join();
    }
}
