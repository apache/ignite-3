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
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.internal.configuration.validation.TestValidationUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SslConfigurationValidatorImplTest {

    @Test
    public void nullKeyStorePath() {
        validate(createKeyStoreConfig("PKCS12", null, "changeIt"),
                "Key store path must not be blank");
    }

    @Test
    public void emptyKeyStorePath() {
        validate(createKeyStoreConfig("PKCS12", "", "changeIt"),
                "Key store path must not be blank");
    }

    @Test
    public void nullKeyStoreType() {
        validate(createKeyStoreConfig(null, "/path/to/keystore.p12", null),
                "Key store type must not be blank", "Key store file doesn't exist at /path/to/keystore.p12");
    }

    @Test
    public void blankKeyStoreType() {
        validate(createKeyStoreConfig("", "/path/to/keystore.p12", null),
                "Key store type must not be blank", "Key store file doesn't exist at /path/to/keystore.p12");
    }

    @Test
    public void nullTrustStorePath(@TempDir Path tmpDir) throws IOException {
        validate(createTrustStoreConfig(tmpDir, "PKCS12", null, "changeIt"),
                "Trust store path must not be blank");
    }

    @Test
    public void emptyTrustStorePath(@TempDir Path tmpDir) throws IOException {
        validate(createTrustStoreConfig(tmpDir, "PKCS12", "", "changeIt"),
                "Trust store path must not be blank");
    }

    @Test
    public void nullTrustStoreType(@TempDir Path tmpDir) throws IOException {
        validate(createTrustStoreConfig(tmpDir, null, "/path/to/keystore.p12", null),
                "Trust store type must not be blank", "Trust store file doesn't exist at /path/to/keystore.p12");
    }

    @Test
    public void blankTrustStoreType(@TempDir Path tmpDir) throws IOException {
        validate(createTrustStoreConfig(tmpDir, "", "/path/to/keystore.p12", null),
                "Trust store type must not be blank", "Trust store file doesn't exist at /path/to/keystore.p12");
    }

    @Test
    public void validKeyStoreConfig(@TempDir Path tmpDir) throws IOException {
        KeyStoreView keyStore = createValidKeyStoreConfig(tmpDir);
        validate(new StubSslView(true, "NONE", keyStore, null), (String[]) null);
    }

    @Test
    public void validTrustStoreConfig(@TempDir Path tmpDir) throws IOException {
        Path trustStorePath = tmpDir.resolve("truststore.jks");
        Files.createFile(trustStorePath);

        validate(createTrustStoreConfig(tmpDir, "JKS", trustStorePath.toAbsolutePath().toString(), null), (String[]) null);
    }

    private static void validate(SslView config, String ... errorMessagePrefixes) {
        var ctx = mockValidationContext(null, config);
        TestValidationUtil.validate(SslConfigurationValidatorImpl.INSTANCE, mock(SslConfigurationValidator.class), ctx,
                errorMessagePrefixes);
    }

    private static SslView createKeyStoreConfig(String type, String path, String password) {
        return new StubSslView(true, "NONE", new StubKeyStoreView(type, path, password), null);
    }

    private static SslView createTrustStoreConfig(Path tmpDir, String type, String path, String password) throws IOException {
        KeyStoreView keyStore = createValidKeyStoreConfig(tmpDir);
        KeyStoreView trustStore = new StubKeyStoreView(type, path, password);
        return new StubSslView(true, "OPTIONAL", keyStore, trustStore);
    }

    private static KeyStoreView createValidKeyStoreConfig(Path tmpDir) throws IOException {
        Path keyStorePath = tmpDir.resolve("keystore.p12");
        Files.createFile(keyStorePath);

        return new StubKeyStoreView("PKCS12", keyStorePath.toAbsolutePath().toString(), null);
    }
}
