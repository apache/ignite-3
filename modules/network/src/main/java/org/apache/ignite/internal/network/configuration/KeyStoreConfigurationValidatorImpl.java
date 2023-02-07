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

package org.apache.ignite.internal.network.configuration;

import static org.apache.ignite.internal.util.StringUtils.nullOrBlank;

import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;

/**
 * Key store configuration validator implementation.
 */
public class KeyStoreConfigurationValidatorImpl implements Validator<KeyStoreConfigurationValidator, KeyStoreView> {

    public static final KeyStoreConfigurationValidatorImpl INSTANCE = new KeyStoreConfigurationValidatorImpl();

    @Override
    public void validate(KeyStoreConfigurationValidator annotation, ValidationContext<KeyStoreView> ctx) {
        KeyStoreView keyStore = ctx.getNewValue();
        String type = keyStore.type();
        String path = keyStore.path();
        String password = keyStore.password();
        if (nullOrBlank(type) && nullOrBlank(path) && nullOrBlank(password)) {
            return;
        } else {
            if (nullOrBlank(type)) {
                ctx.addIssue(new ValidationIssue(
                        ctx.currentKey(),
                        "Key store type must not be blank"
                ));
            }

            if (nullOrBlank(path)) {
                ctx.addIssue(new ValidationIssue(
                        ctx.currentKey(),
                        "Key store path must not be blank"
                ));
            }
        }
    }
}
