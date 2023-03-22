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

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.net.ssl.SSLException;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * SSL configuration validator implementation.
 */
public class SslConfigurationValidatorImpl implements Validator<SslConfigurationValidator, AbstractSslView> {

    public static final SslConfigurationValidatorImpl INSTANCE = new SslConfigurationValidatorImpl();

    private static final IgniteLogger LOG = Loggers.forClass(SslConfigurationValidatorImpl.class);

    @Override
    public void validate(SslConfigurationValidator annotation, ValidationContext<AbstractSslView> ctx) {
        AbstractSslView ssl = ctx.getNewValue();
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

            if (!ssl.ciphers().isBlank()) {
                validateCiphers(ctx, ssl);
            }
        }
    }

    private static void validateKeyStore(ValidationContext<AbstractSslView> ctx, String keyName, String type, KeyStoreView keyStore) {
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

    private static void validateCiphers(ValidationContext<AbstractSslView> ctx, AbstractSslView ssl) {
        try {
            SslContext context = SslContextBuilder.forClient().build();
            Set<String> supported = Arrays.stream(context.newEngine(ByteBufAllocator.DEFAULT).getSupportedCipherSuites())
                    .filter(Objects::nonNull) // OpenSSL engine returns null string in the array so we need to filter them out
                    .collect(Collectors.toSet());
            Set<String> ciphers = Arrays.stream(ssl.ciphers().split(","))
                    .map(String::strip)
                    .collect(Collectors.toSet());

            // If removeAll returns true, it means that there were at least some supported ciphers.
            boolean haveSupported = ciphers.removeAll(supported);
            if (!ciphers.isEmpty()) {
                if (!haveSupported) {
                    ctx.addIssue(new ValidationIssue(ctx.currentKey(), "None of the configured cipher suites are supported: " + ciphers));
                }
                LOG.info("Some of the configured cipher suites are unsupported: {}", ciphers);
            }
        } catch (SSLException e) {
            ctx.addIssue(new ValidationIssue(ctx.currentKey(), "Can't create SSL engine"));
            LOG.warn("Can't create SSL engine", e);
        }
    }
}
