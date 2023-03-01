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

package org.apache.ignite.internal.configuration;

import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.util.StringUtils;
import org.apache.ignite.rest.AuthenticationType;

/**
 * Authentication provider schema configuration validator implementation.
 */
public class AuthenticationProvidersValidatorImpl implements
        Validator<AuthenticationProvidersValidator, NamedListView<AuthenticationProviderView>> {

    public static final AuthenticationProvidersValidatorImpl INSTANCE = new AuthenticationProvidersValidatorImpl();

    @Override
    public void validate(AuthenticationProvidersValidator annotation, ValidationContext<NamedListView<AuthenticationProviderView>> ctx) {
        NamedListView<AuthenticationProviderView> providers = ctx.getNewValue();
        providers.namedListKeys()
                .forEach(it -> validateProvider(it, providers.get(it), ctx));

    }

    private void validateProvider(String key, AuthenticationProviderView view,
            ValidationContext<NamedListView<AuthenticationProviderView>> ctx) {
        AuthenticationType authenticationType = AuthenticationType.parse(view.type());
        if (authenticationType == null) {
            ctx.addIssue(new ValidationIssue(key, "Unknown auth type: " + view.type()));
        } else {
            if (authenticationType == AuthenticationType.BASIC) {
                BasicAuthenticationProviderView basicAuthProviderView = (BasicAuthenticationProviderView) view;
                if (StringUtils.nullOrBlank(basicAuthProviderView.login())) {
                    ctx.addIssue(new ValidationIssue(key, "Login must not be blank"));
                }
                if (StringUtils.nullOrBlank(basicAuthProviderView.password())) {
                    ctx.addIssue(new ValidationIssue(key, "Password must not be blank"));
                }
            }
        }
    }
}
