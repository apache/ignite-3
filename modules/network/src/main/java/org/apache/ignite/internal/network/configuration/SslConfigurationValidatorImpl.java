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

import io.netty.handler.ssl.ClientAuth;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;

/**
 * SSL configuration validator implementation.
 */
public class SslConfigurationValidatorImpl implements Validator<SslConfigurationValidator, SslView> {

    public static final SslConfigurationValidatorImpl INSTANCE = new SslConfigurationValidatorImpl();

    @Override
    public void validate(SslConfigurationValidator annotation, ValidationContext<SslView> ctx) {
        SslView ssl = ctx.getNewValue();
        if (ssl.enabled()) {
            validateKeyStore(ctx, ".keyStore", "Key store", ssl.keyStore());

            try {
                ClientAuth clientAuth = ClientAuth.valueOf(ssl.clientAuth().toUpperCase());
                if (clientAuth != ClientAuth.NONE) {
                    validateKeyStore(ctx, ".trustStore", "Trust store", ssl.trustStore());
                }
            } catch (IllegalArgumentException e) {
                ctx.addIssue(new ValidationIssue(ctx.currentKey(), "Incorrect client auth parameter " + ssl.clientAuth()));
            }
        }
    }

    private static void validateKeyStore(ValidationContext<SslView> ctx, String keyName, String type, KeyStoreView keyStore) {
        String keyStorePath = keyStore.path();
        if (nullOrBlank(keyStorePath) && nullOrBlank(keyStore.password())) {
            return;
        }

        if (nullOrBlank(keyStore.type())) {
            ctx.addIssue(new ValidationIssue(ctx.currentKey() + keyName, type + " type must not be blank"));
        }

        if (nullOrBlank(keyStorePath)) {
            ctx.addIssue(new ValidationIssue(ctx.currentKey() + keyName, type + " path must not be blank"));
        } else {
            try {
                if (!Files.exists(Path.of(keyStorePath))) {
                    ctx.addIssue(new ValidationIssue(ctx.currentKey() + keyName, type + " file doesn't exist at " + keyStorePath));
                }
            } catch (InvalidPathException e) {
                ctx.addIssue(new ValidationIssue(ctx.currentKey() + keyName, type + " file path is incorrect: " + keyStorePath));
            }
        }
    }
}
