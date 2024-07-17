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
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(WorkDirectoryExtension.class)
class SslConfigurationValidatorImplTest extends BaseIgniteAbstractTest {

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
    public void allCiphersAreIncompatible(@WorkDirectory Path workDir) throws IOException {
        KeyStoreView keyStore = createValidKeyStoreConfig(workDir);
        validate(new StubSslView(true, "NONE", "foo", keyStore, null),
                "None of the configured cipher suites are supported: [foo]");
    }

    @Test
    public void someCiphersAreIncompatible(@WorkDirectory Path workDir) throws IOException {
        KeyStoreView keyStore = createValidKeyStoreConfig(workDir);
        validate(new StubSslView(true, "NONE", "foo, TLS_AES_256_GCM_SHA384", keyStore, null),
                (String[]) null);
    }

    @Test
    public void allCiphersAreCompatible(@WorkDirectory Path workDir) throws IOException {
        KeyStoreView keyStore = createValidKeyStoreConfig(workDir);
        validate(new StubSslView(true, "NONE", "TLS_AES_256_GCM_SHA384", keyStore, null),
                (String[]) null);
    }

    @Test
    public void nullTrustStorePath(@WorkDirectory Path workDir) throws IOException {
        validate(createTrustStoreConfig(workDir, "PKCS12", null, "changeIt"),
                "Trust store path must not be blank");
    }

    @Test
    public void emptyTrustStorePath(@WorkDirectory Path workDir) throws IOException {
        validate(createTrustStoreConfig(workDir, "PKCS12", "", "changeIt"),
                "Trust store path must not be blank");
    }

    @Test
    public void nullTrustStoreType(@WorkDirectory Path workDir) throws IOException {
        validate(createTrustStoreConfig(workDir, null, "/path/to/keystore.p12", null),
                "Trust store type must not be blank", "Trust store file doesn't exist at /path/to/keystore.p12");
    }

    @Test
    public void blankTrustStoreType(@WorkDirectory Path workDir) throws IOException {
        validate(createTrustStoreConfig(workDir, "", "/path/to/keystore.p12", null),
                "Trust store type must not be blank", "Trust store file doesn't exist at /path/to/keystore.p12");
    }

    @Test
    public void incorrectAuthType(@WorkDirectory Path workDir) throws IOException {
        KeyStoreView keyStore = createValidKeyStoreConfig(workDir);
        StubSslView sslView = new StubSslView(true, "foo", "", keyStore, null);

        validate(sslView, "Incorrect client auth parameter foo");
    }

    @Test
    public void validKeyStoreConfig(@WorkDirectory Path workDir) throws IOException {
        KeyStoreView keyStore = createValidKeyStoreConfig(workDir);
        validate(new StubSslView(true, "NONE", "", keyStore, null), (String[]) null);
    }

    @Test
    public void validTrustStoreConfig(@WorkDirectory Path workDir) throws IOException {
        Path trustStorePath = workDir.resolve("truststore.jks");
        Files.createFile(trustStorePath);

        validate(createTrustStoreConfig(workDir, "JKS", trustStorePath.toAbsolutePath().toString(), null), (String[]) null);
    }

    private static void validate(AbstractSslView config, String ... errorMessagePrefixes) {
        var ctx = mockValidationContext(null, config);
        TestValidationUtil.validate(SslConfigurationValidatorImpl.INSTANCE, mock(SslConfigurationValidator.class), ctx,
                errorMessagePrefixes);
    }

    private static AbstractSslView createKeyStoreConfig(String type, String path, String password) {
        return new StubSslView(true, "NONE", "", new StubKeyStoreView(type, path, password), null);
    }

    private static AbstractSslView createTrustStoreConfig(Path workDir, String type, String path, String password) throws IOException {
        KeyStoreView keyStore = createValidKeyStoreConfig(workDir);
        KeyStoreView trustStore = new StubKeyStoreView(type, path, password);
        return new StubSslView(true, "OPTIONAL", "", keyStore, trustStore);
    }

    private static KeyStoreView createValidKeyStoreConfig(Path workDir) throws IOException {
        Path keyStorePath = workDir.resolve("keystore.p12");
        Files.createFile(keyStorePath);

        return new StubKeyStoreView("PKCS12", keyStorePath.toAbsolutePath().toString(), null);
    }
}
