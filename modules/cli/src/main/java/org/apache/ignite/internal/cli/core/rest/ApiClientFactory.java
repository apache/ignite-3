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

package org.apache.ignite.internal.cli.core.rest;

import static org.apache.ignite.internal.cli.config.CliConfigKeys.BASIC_AUTHENTICATION_LOGIN;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.BASIC_AUTHENTICATION_PASSWORD;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.REST_KEY_STORE_PASSWORD;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.REST_KEY_STORE_PATH;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.REST_TRUST_STORE_PASSWORD;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.REST_TRUST_STORE_PATH;
import static org.apache.ignite.internal.util.StringUtils.nullOrBlank;

import jakarta.inject.Singleton;
import java.io.File;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.OkHttpClient.Builder;
import okhttp3.internal.tls.OkHostnameVerifier;
import org.apache.ignite.internal.cli.config.ConfigManager;
import org.apache.ignite.internal.cli.config.ConfigManagerProvider;
import org.apache.ignite.internal.cli.core.exception.IgniteCliException;
import org.apache.ignite.internal.cli.logger.CliLoggers;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.jetbrains.annotations.Nullable;

/**
 * Factory for {@link ApiClient}. This class holds the map from {@link ApiClientSettings} to {@link ApiClient}. If settings are changed, the
 * factory will recreate a new client. Otherwise, returns a client from the cache.
 */
@Singleton
public class ApiClientFactory {

    private final Map<ApiClientSettings, ApiClient> clientMap = new ConcurrentHashMap<>();

    private final ConfigManagerProvider configManagerProvider;

    public ApiClientFactory(ConfigManagerProvider configManagerProvider) {
        this.configManagerProvider = configManagerProvider;
    }

    /**
     * Returns {@link ApiClient} for the base path.
     *
     * @param path Base path.
     * @return created API client.
     */
    public ApiClient getClient(String path) {
        ApiClient apiClient = clientMap.computeIfAbsent(settings(path), this::buildClient);
        CliLoggers.addApiClient(path, apiClient);
        return apiClient;
    }

    private ApiClientSettings settings(String path) {
        ConfigManager configManager = configManagerProvider.get();
        return ApiClientSettings.builder()
                .basePath(path)
                .keyStorePath(configManager.getCurrentProperty(REST_KEY_STORE_PATH.value()))
                .keyStorePassword(configManager.getCurrentProperty(REST_KEY_STORE_PASSWORD.value()))
                .trustStorePath(configManager.getCurrentProperty(REST_TRUST_STORE_PATH.value()))
                .trustStorePassword(configManager.getCurrentProperty(REST_TRUST_STORE_PASSWORD.value()))
                .basicAuthLogin(configManager.getCurrentProperty(BASIC_AUTHENTICATION_LOGIN.value()))
                .basicAuthPassword(configManager.getCurrentProperty(BASIC_AUTHENTICATION_PASSWORD.value()))
                .build();
    }


    private ApiClient buildClient(ApiClientSettings settings) {
        try {
            Builder builder = new Builder();

            if (!nullOrBlank(settings.keyStorePath()) || !nullOrBlank(settings.keyStorePassword())) {
                applySslSettings(builder, settings);
            }

            Interceptor authInterceptor = authInterceptor(settings);
            if (authInterceptor != null) {
                builder.addInterceptor(authInterceptor);
            }

            OkHttpClient okHttpClient = builder.build();

            return new ApiClient(okHttpClient)
                    .setBasePath(settings.basePath());

        } catch (Exception e) {
            throw new IgniteCliException("Couldn't build REST client", e);
        }
    }

    private Builder applySslSettings(Builder builder, ApiClientSettings settings) throws UnrecoverableKeyException,
            CertificateException,
            NoSuchAlgorithmException,
            KeyStoreException,
            IOException,
            KeyManagementException {

        TrustManagerFactory trustManagerFactory = trustManagerFactory(settings);
        KeyManagerFactory keyManagerFactory = keyManagerFactory(settings);

        TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
        KeyManager[] keyManagers = keyManagerFactory.getKeyManagers();

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagers, trustManagers, new SecureRandom());
        return builder.sslSocketFactory(sslContext.getSocketFactory(), (X509TrustManager) trustManagers[0])
                .hostnameVerifier(OkHostnameVerifier.INSTANCE);
    }

    private KeyManagerFactory keyManagerFactory(ApiClientSettings settings)
            throws NoSuchAlgorithmException, KeyStoreException, UnrecoverableKeyException, CertificateException, IOException {
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

        if (nullOrBlank(settings.keyStorePath())) {
            keyManagerFactory.init(null, null);
        } else {
            char[] password = settings.keyStorePassword() == null ? null : settings.keyStorePassword().toCharArray();
            KeyStore keyStore = KeyStore.getInstance(new File(settings.keyStorePath()), password);
            keyManagerFactory.init(keyStore, settings.keyStorePassword().toCharArray());
        }

        return keyManagerFactory;
    }

    private TrustManagerFactory trustManagerFactory(ApiClientSettings settings)
            throws NoSuchAlgorithmException, KeyStoreException, CertificateException, IOException {
        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

        if (nullOrBlank(settings.trustStorePath())) {
            trustManagerFactory.init((KeyStore) null);
        } else {
            char[] password = settings.trustStorePassword() == null ? null : settings.trustStorePassword().toCharArray();
            KeyStore trustStore = KeyStore.getInstance(new File(settings.trustStorePath()), password);
            trustManagerFactory.init(trustStore);
        }

        return trustManagerFactory;
    }

    @Nullable
    private Interceptor authInterceptor(ApiClientSettings settings) {
        if (!nullOrBlank(settings.basicAuthLogin()) && !nullOrBlank(settings.basicAuthPassword())) {
            return new BasicAuthenticationInterceptor(settings.basicAuthLogin(), settings.basicAuthPassword());
        } else {
            return null;
        }
    }
}
