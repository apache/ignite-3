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
import static org.mockito.Mockito.mock;

import org.apache.ignite.configuration.validation.ValidationContext;
import org.junit.jupiter.api.Test;

class KeyStoreConfigurationValidatorImplTest {

    @Test
    public void nullPath() {
        ValidationContext<KeyStoreView> ctx = mockValidationContext(
                null,
                new StubKeyStoreView("PKCS12", null, "changeIt")
        );
        validate(KeyStoreConfigurationValidatorImpl.INSTANCE, mock(KeyStoreConfigurationValidator.class), ctx,
                "Key store path must not be blank");
    }

    @Test
    public void emptyPath() {
        ValidationContext<KeyStoreView> ctx = mockValidationContext(
                null,
                new StubKeyStoreView("PKCS12", "", "changeIt")
        );
        validate(KeyStoreConfigurationValidatorImpl.INSTANCE, mock(KeyStoreConfigurationValidator.class), ctx,
                "Key store path must not be blank");
    }

    @Test
    public void nullType() {
        ValidationContext<KeyStoreView> ctx = mockValidationContext(
                null,
                new StubKeyStoreView(null, "/path/to/keystore.p12", null)
        );
        validate(KeyStoreConfigurationValidatorImpl.INSTANCE, mock(KeyStoreConfigurationValidator.class), ctx,
                "Key store type must not be blank");
    }

    @Test
    public void emptyType() {
        ValidationContext<KeyStoreView> ctx = mockValidationContext(
                null,
                new StubKeyStoreView("", "/path/to/keystore.p12", null)
        );
        validate(KeyStoreConfigurationValidatorImpl.INSTANCE, mock(KeyStoreConfigurationValidator.class), ctx,
                "Key store type must not be blank");
    }

    @Test
    public void validConfig() {
        ValidationContext<KeyStoreView> ctx = mockValidationContext(
                null,
                new StubKeyStoreView("PKCS12", "/path/to/keystore.p12", null)
        );
        validate(KeyStoreConfigurationValidatorImpl.INSTANCE, mock(KeyStoreConfigurationValidator.class), ctx, null);
    }
}
