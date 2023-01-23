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

import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;

/**
 * Auth schema configuration validator implementation.
 */
public class AuthConfigurationValidatorImpl implements Validator<AuthConfigurationValidator, AuthView> {

    public static final AuthConfigurationValidatorImpl INSTANCE = new AuthConfigurationValidatorImpl();

    @Override
    public void validate(AuthConfigurationValidator annotation, ValidationContext<AuthView> ctx) {
        AuthView view = ctx.getNewValue();

        if (view == null) {
            ctx.addIssue(new ValidationIssue(ctx.currentKey(), "Auth config must not be null"));
            return;
        }

        if (view.enabled() && view.providers().size() == 0) {
            ctx.addIssue(new ValidationIssue(ctx.currentKey(), "Providers must be present, if auth is enabled"));
        }
    }
}
