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

import static org.apache.commons.lang3.StringUtils.isBlank;

import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.rest.AuthType;

/**
 * Auth provider schema configuration validator implementation.
 */
public class AuthProvidersValidatorImpl implements Validator<AuthProvidersValidator, NamedListView<AuthProviderView>> {

    public static final AuthProvidersValidatorImpl INSTANCE = new AuthProvidersValidatorImpl();

    @Override
    public void validate(AuthProvidersValidator annotation, ValidationContext<NamedListView<AuthProviderView>> ctx) {
        NamedListView<AuthProviderView> providers = ctx.getNewValue();
        providers.namedListKeys()
                .forEach(it -> validateProvider(it, providers.get(it), ctx));

    }

    private void validateProvider(String key, AuthProviderView view, ValidationContext<NamedListView<AuthProviderView>> ctx) {
        AuthType authType = AuthType.parse(view.type());
        if (authType == null) {
            ctx.addIssue(new ValidationIssue(key, "Unknown auth type: " + view.type()));
        } else {
            if (authType == AuthType.BASIC) {
                BasicAuthProviderView basicAuthProviderView = (BasicAuthProviderView) view;
                if (isBlank(basicAuthProviderView.login())) {
                    ctx.addIssue(new ValidationIssue(key, "Login must not be blank"));
                }
                if (isBlank(basicAuthProviderView.password())) {
                    ctx.addIssue(new ValidationIssue(key, "Password must not be blank"));
                }
            }
        }
    }
}
