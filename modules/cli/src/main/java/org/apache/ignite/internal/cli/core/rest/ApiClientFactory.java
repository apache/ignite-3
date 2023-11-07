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

import static org.apache.ignite.internal.cli.config.CliConfigKeys.BASIC_AUTHENTICATION_PASSWORD;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.BASIC_AUTHENTICATION_USERNAME;
import static org.apache.ignite.internal.cli.config.CliConfigKeys.REST_CIPHERS;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import okhttp3.ConnectionSpec;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import okhttp3.OkHttpClient.Builder;
import okhttp3.internal.tls.OkHostnameVerifier;
import org.apache.ignite.internal.cli.config.ConfigManager;
import org.apache.ignite.internal.cli.config.ConfigManagerProvider;
import org.apache.ignite.internal.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.internal.cli.logger.CliLoggers;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.jetbrains.annotations.Nullable;

/**
 * Factory for {@link ApiClient}. This class holds the map from {@link ApiClientSettings} to {@link ApiClient}. If settings are changed, the
 * factory will recreate a new client. Otherwise, returns a client from the cache.
 */
@Singleton
public class ApiClientFactory {

    private static final Pattern INCORRECT_PASSWORD_PATTERN = Pattern.compile(".*keystore password was incorrect.*");

    private final Map<ApiClientSettings, ApiClient> clientMap = new ConcurrentHashMap<>();

    private final AtomicReference<ApiClientSettings> currentSessionSettings = new AtomicReference<>();

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
        return getClientFromSettings(settingsWithAuth(path));
    }

    private ApiClient getClientFromSettings(ApiClientSettings settings) {
        ApiClient apiClient = clientMap.computeIfAbsent(settings, ApiClientFactory::buildClient);
        CliLoggers.addApiClient(settings.basePath(), apiClient);
        return apiClient;
    }

    private ApiClientSettings settingsWithAuth(String path) {
        ConfigManager configManager = configManagerProvider.get();
        ApiClientSettingsBuilder builder = ApiClientSettings.builder()
                .basePath(path)
                .keyStorePath(configManager.getCurrentProperty(REST_KEY_STORE_PATH.value()))
                .keyStorePassword(configManager.getCurrentProperty(REST_KEY_STORE_PASSWORD.value()))
                .trustStorePath(configManager.getCurrentProperty(REST_TRUST_STORE_PATH.value()))
                .trustStorePassword(configManager.getCurrentProperty(REST_TRUST_STORE_PASSWORD.value()))
                .ciphers(configManager.getCurrentProperty(REST_CIPHERS.value()));
        return setupAuthentication(builder).build();
    }

    private ApiClientSettingsBuilder setupAuthentication(ApiClientSettingsBuilder builder) {
        ConfigManager configManager = configManagerProvider.get();

        // Use credentials from current session settings if exist.
        ApiClientSettings currentCredentialsSettings = currentSessionSettings();
        String username = currentCredentialsSettings != null
                ? currentCredentialsSettings.basicAuthenticationUsername()
                : configManager.getCurrentProperty(BASIC_AUTHENTICATION_USERNAME.value());
        String password = currentCredentialsSettings != null
                ? currentCredentialsSettings.basicAuthenticationPassword()
                : configManager.getCurrentProperty(BASIC_AUTHENTICATION_PASSWORD.value());
        builder
                .basicAuthenticationUsername(username)
                .basicAuthenticationPassword(password);

        return builder;
    }

    /**
     * Builds {@link ApiClient} using provided settings.
     *
     * @param settings Settings.
     * @return Created client.
     */
    public static ApiClient buildClient(ApiClientSettings settings) {
        try {
            Builder builder = new Builder();

            if (!nullOrBlank(settings.trustStorePath()) || !nullOrBlank(settings.trustStorePassword())) {
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
            throw new IgniteCliApiException(e, settings.basePath());
        }
    }

    /**
     * Set api client settings for current session.
     *
     * @param settings api client settings
     */
    public void setSessionSettings(@Nullable ApiClientSettings settings) {
        if (settings != null) {
            currentSessionSettings.compareAndSet(null, settings);
        } else {
            currentSessionSettings.set(null);
        }
    }

    public ApiClientSettings currentSessionSettings() {
        return currentSessionSettings.get();
    }

    private static Builder applySslSettings(Builder builder, ApiClientSettings settings) throws UnrecoverableKeyException,
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

        setCiphers(builder, settings);

        return builder.sslSocketFactory(sslContext.getSocketFactory(), (X509TrustManager) trustManagers[0])
                .hostnameVerifier(OkHostnameVerifier.INSTANCE);
    }

    private static KeyManagerFactory keyManagerFactory(ApiClientSettings settings)
            throws NoSuchAlgorithmException, KeyStoreException, UnrecoverableKeyException, CertificateException, IOException {
        try {
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

            if (nullOrBlank(settings.keyStorePath())) {
                keyManagerFactory.init(null, null);
            } else {
                char[] password = settings.keyStorePassword() == null ? null : settings.keyStorePassword().toCharArray();
                KeyStore keyStore = KeyStore.getInstance(new File(settings.keyStorePath()), password);
                keyManagerFactory.init(keyStore, password);
            }

            return keyManagerFactory;
        } catch (IOException e) {
            if (INCORRECT_PASSWORD_PATTERN.matcher(e.getMessage()).matches()) {
                throw new IOException("Key-store password was incorrect", e.getCause());
            } else {
                throw e;
            }
        }
    }

    private static TrustManagerFactory trustManagerFactory(ApiClientSettings settings)
            throws NoSuchAlgorithmException, KeyStoreException, CertificateException, IOException {
        try {
            TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

            if (nullOrBlank(settings.trustStorePath())) {
                trustManagerFactory.init((KeyStore) null);
            } else {
                char[] password = settings.trustStorePassword() == null ? null : settings.trustStorePassword().toCharArray();
                KeyStore trustStore = KeyStore.getInstance(new File(settings.trustStorePath()), password);
                trustManagerFactory.init(trustStore);
            }

            return trustManagerFactory;
        } catch (IOException e) {
            if (INCORRECT_PASSWORD_PATTERN.matcher(e.getMessage()).matches()) {
                throw new IOException("Trust-store password was incorrect", e.getCause());
            } else {
                throw e;
            }
        }
    }

    private static void setCiphers(Builder builder, ApiClientSettings settings) {
        if (!nullOrBlank(settings.ciphers())) {
            List<String> cipherSuites = Arrays.stream(settings.ciphers().split(","))
                    .map(String::strip)
                    .collect(Collectors.toList());
            ConnectionSpec spec = new ConnectionSpec.Builder(true)
                    .cipherSuites(cipherSuites.toArray(String[]::new))
                    .build();
            builder.connectionSpecs(List.of(spec));
        }
    }

    @Nullable
    private static Interceptor authInterceptor(ApiClientSettings settings) {
        if (!nullOrBlank(settings.basicAuthenticationUsername()) && !nullOrBlank(settings.basicAuthenticationPassword())) {
            return new BasicAuthenticationInterceptor(settings.basicAuthenticationUsername(), settings.basicAuthenticationPassword());
        } else {
            return null;
        }
    }
}
