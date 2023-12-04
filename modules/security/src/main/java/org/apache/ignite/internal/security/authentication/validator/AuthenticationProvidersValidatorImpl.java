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

package org.apache.ignite.internal.security.authentication.validator;

import static org.apache.ignite.internal.security.authentication.SecurityConfigurationModule.DEFAULT_PROVIDER_NAME;

import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.security.authentication.basic.BasicAuthenticationProviderConfiguration;
import org.apache.ignite.internal.security.authentication.basic.BasicAuthenticationProviderView;
import org.apache.ignite.internal.security.authentication.configuration.AuthenticationProviderView;
import org.apache.ignite.internal.security.authentication.configuration.validator.AuthenticationProvidersValidator;
import org.apache.ignite.internal.security.configuration.SecurityConfiguration;

/**
 * Implementation of {@link AuthenticationProvidersValidator}.
 */
public class AuthenticationProvidersValidatorImpl implements
        Validator<AuthenticationProvidersValidator, NamedListView<? extends AuthenticationProviderView>> {
    public static final AuthenticationProvidersValidatorImpl INSTANCE = new AuthenticationProvidersValidatorImpl();

    @Override
    public void validate(
            AuthenticationProvidersValidator annotation,
            ValidationContext<NamedListView<? extends AuthenticationProviderView>> ctx
    ) {
        boolean enabled = ctx.getNewRoot(SecurityConfiguration.KEY).enabled();
        NamedListView<? extends AuthenticationProviderView> view = ctx.getNewValue();

        BasicAuthenticationProviderView basicProvider = (BasicAuthenticationProviderView) view.get(DEFAULT_PROVIDER_NAME);
        if (basicProvider == null) {
            ctx.addIssue(new ValidationIssue(ctx.currentKey(), "Default provider " + DEFAULT_PROVIDER_NAME + " is not removable."));
            return;
        }

        if (checkOnlyOneBasicProvider(ctx, view)) {
            return;
        }

        if (enabled && view.size() == 1 && basicProvider.users().size() == 0) {
            ctx.addIssue(new ValidationIssue(ctx.currentKey(), "Basic provider must have at least one user "
                    + "in case when no other providers present."));
        }
    }

    private static boolean checkOnlyOneBasicProvider(
            ValidationContext<NamedListView<? extends AuthenticationProviderView>> ctx,
            NamedListView<? extends AuthenticationProviderView> view
    ) {
        boolean basicAlreadyFound = false;
        for (AuthenticationProviderView authenticationProviderView : view) {
            if (authenticationProviderView instanceof BasicAuthenticationProviderConfiguration) {
                if (basicAlreadyFound) {
                    ctx.addIssue(new ValidationIssue(ctx.currentKey(), "Only one basic provider supported."));
                    return true;
                }
                basicAlreadyFound = true;
            }
        }
        return false;
    }
}
