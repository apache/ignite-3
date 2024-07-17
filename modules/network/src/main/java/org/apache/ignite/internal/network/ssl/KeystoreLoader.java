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

package org.apache.ignite.internal.network.ssl;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import org.apache.ignite.internal.network.configuration.KeyStoreView;

/** Java keystore loader. */
public final class KeystoreLoader {

    private KeystoreLoader() {
    }

    /** Initialize and load keystore with provided configuration. */
    public static KeyStore load(KeyStoreView keyStoreConfiguration)
            throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException {
        char[] password = keyStoreConfiguration.password() == null ? null : keyStoreConfiguration.password().toCharArray();

        KeyStore ks = KeyStore.getInstance(keyStoreConfiguration.type());
        try (var is = Files.newInputStream(Path.of(keyStoreConfiguration.path()))) {
            ks.load(is, password);
        }

        return ks;
    }
}
