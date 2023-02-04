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

import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.mockValidationContext;
import static org.apache.ignite.internal.configuration.validation.TestValidationUtil.validate;
import static org.apache.ignite.internal.network.configuration.TestUtils.stubKeyStoreView;
import static org.apache.ignite.internal.network.configuration.TestUtils.stubSslView;
import static org.mockito.Mockito.mock;

import org.apache.ignite.configuration.validation.ValidationContext;
import org.junit.jupiter.api.Test;

class SslConfigurationValidatorImplTest {

    @Test
    public void enabledSslWithoutKeyStoreAndTrustStore() {
        ValidationContext<SslView> ctx = mockValidationContext(
                null,
                stubSslView(true, null, null)
        );
        validate(SslConfigurationValidatorImpl.INSTANCE, mock(SslConfigurationValidator.class), ctx,
                "At least one of keyStore or trustStore must be specified");
    }

    @Test
    public void enabledSslWithoutKeyStore() {
        SslView newValue = stubSslView(
                true,
                null,
                stubKeyStoreView(KeyStoreType.PKCS12.toString(), "test-key-store", "test-key-store-password")
        );
        ValidationContext<SslView> ctx = mockValidationContext(null, newValue);
        validate(SslConfigurationValidatorImpl.INSTANCE, mock(SslConfigurationValidator.class), ctx, null);
    }

    @Test
    public void enabledSslWithoutTrustStore() {
        SslView newValue = stubSslView(
                true,
                stubKeyStoreView(KeyStoreType.PKCS12.toString(), "test-key-store", "test-key-store-password"),
                null
        );
        ValidationContext<SslView> ctx = mockValidationContext(null, newValue);
        validate(SslConfigurationValidatorImpl.INSTANCE, mock(SslConfigurationValidator.class), ctx, null);
    }

    @Test
    public void enabledSslWithKeyStoreAndTrustStore() {
        SslView newValue = stubSslView(
                true,
                stubKeyStoreView(KeyStoreType.PKCS12.toString(), "test-key-store", "test-key-store-password"),
                stubKeyStoreView(KeyStoreType.PKCS12.toString(), "test-trust-store", "test-trust-store-password")
        );
        ValidationContext<SslView> ctx = mockValidationContext(null, newValue);
        validate(SslConfigurationValidatorImpl.INSTANCE, mock(SslConfigurationValidator.class), ctx, null);
    }

    @Test
    public void disabled() {
        SslView newValue = stubSslView(false, null, null);
        ValidationContext<SslView> ctx = mockValidationContext(null, newValue);
        validate(SslConfigurationValidatorImpl.INSTANCE, mock(SslConfigurationValidator.class), ctx, null);
    }
}
