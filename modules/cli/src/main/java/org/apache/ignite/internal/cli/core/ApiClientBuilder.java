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

package org.apache.ignite.internal.cli.core;

import static org.apache.ignite.internal.util.StringUtils.nullOrBlank;

import java.io.File;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import okhttp3.OkHttpClient;
import okhttp3.OkHttpClient.Builder;
import okhttp3.internal.tls.OkHostnameVerifier;
import org.apache.ignite.rest.client.invoker.ApiClient;

class ApiClientBuilder {
    private String basePath;
    private String keyStorePath;
    private String keyStorePassword;
    private String trustStorePath;
    private String trustStorePassword;

    static ApiClientBuilder create() {
        return new ApiClientBuilder();
    }

    ApiClientBuilder basePath(String basePath) {
        this.basePath = basePath;
        return this;
    }

    ApiClientBuilder keyStorePath(String keyStorePath) {
        this.keyStorePath = keyStorePath;
        return this;
    }

    ApiClientBuilder keyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
        return this;
    }

    ApiClientBuilder trustStorePath(String trustStorePath) {
        this.trustStorePath = trustStorePath;
        return this;
    }

    ApiClientBuilder trustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
        return this;
    }

    ApiClient build() throws CertificateException,
            NoSuchAlgorithmException,
            KeyStoreException,
            IOException,
            UnrecoverableKeyException,
            KeyManagementException {

        Builder builder = new Builder();

        if (!nullOrBlank(keyStorePath) || !nullOrBlank(trustStorePath)) {
            applySslSettings(builder);
        }

        OkHttpClient okHttpClient = builder.build();

        return new ApiClient(okHttpClient)
                .setBasePath(basePath);
    }

    private Builder applySslSettings(Builder builder) throws UnrecoverableKeyException,
            CertificateException,
            NoSuchAlgorithmException,
            KeyStoreException,
            IOException,
            KeyManagementException {

        TrustManagerFactory trustManagerFactory = trustManagerFactory();
        KeyManagerFactory keyManagerFactory = keyManagerFactory();

        TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
        KeyManager[] keyManagers = keyManagerFactory.getKeyManagers();

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagers, trustManagers, new SecureRandom());
        return builder.sslSocketFactory(sslContext.getSocketFactory(), (X509TrustManager) trustManagers[0])
                .hostnameVerifier(OkHostnameVerifier.INSTANCE);
    }

    private KeyManagerFactory keyManagerFactory()
            throws NoSuchAlgorithmException, KeyStoreException, UnrecoverableKeyException, CertificateException, IOException {
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

        if (nullOrBlank(keyStorePath)) {
            keyManagerFactory.init(null, null);
        } else {
            char[] password = keyStorePassword == null ? null : keyStorePassword.toCharArray();
            KeyStore keyStore = KeyStore.getInstance(new File(keyStorePath), password);
            keyManagerFactory.init(keyStore, keyStorePassword.toCharArray());
        }

        return keyManagerFactory;
    }

    private TrustManagerFactory trustManagerFactory()
            throws NoSuchAlgorithmException, KeyStoreException, CertificateException, IOException {
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

        if (nullOrBlank(trustStorePath)) {
            trustManagerFactory.init((KeyStore) null);
        } else {
            char[] password = trustStorePassword == null ? null : trustStorePassword.toCharArray();
            KeyStore trustStore = KeyStore.getInstance(new File(trustStorePath), password);
            trustManagerFactory.init(trustStore);
        }

        return trustManagerFactory;
    }
}
