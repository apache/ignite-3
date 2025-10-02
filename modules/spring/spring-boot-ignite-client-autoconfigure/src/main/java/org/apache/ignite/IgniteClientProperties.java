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

package org.apache.ignite;

import java.util.concurrent.Executor;
import org.apache.ignite.client.IgniteClientAddressFinder;
import org.apache.ignite.client.IgniteClientAuthenticator;
import org.apache.ignite.client.RetryPolicy;
import org.apache.ignite.lang.LoggerFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Describes properties for Ignite client.
 * See also {@link org.apache.ignite.client.IgniteClientConfiguration}.
 */
@ConfigurationProperties(prefix = "ignite.client")
public class IgniteClientProperties {

    private String[] addresses;
    private RetryPolicy retryPolicy;
    private LoggerFactory loggerFactory;
    private Long connectTimeout;
    private IgniteClientAddressFinder addressFinder;
    private SslConfigurationProperties sslConfiguration;
    private Boolean metricsEnabled;
    private IgniteClientAuthenticator authenticator;
    private AuthenticationProperties auth;
    private Long operationTimeout;
    private Long backgroundReconnectInterval;
    private Executor asyncContinuationExecutor;
    private Long heartbeatInterval;
    private Long heartbeatTimeout;

    /**
     * Gets connection addresses.
     */
    public String[] getAddresses() {
        return addresses;
    }

    /**
     * Sets connection addresses.
     */
    public void setAddresses(String[] addresses) {
        this.addresses = addresses;
    }

    /**
     * Gets retry policy.
     */
    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    /**
     * Sets retry policy.
     */
    public void setRetryPolicy(RetryPolicy retryPolicy) {
        this.retryPolicy = retryPolicy;
    }

    /**
     * Gets logger factory.
     */
    public LoggerFactory getLoggerFactory() {
        return loggerFactory;
    }

    /**
     * Sets logger factory.
     */
    public void setLoggerFactory(LoggerFactory loggerFactory) {
        this.loggerFactory = loggerFactory;
    }

    /**
     * Sets connection timeout.
     */
    public Long getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Gets connection timeout.
     */
    public void setConnectTimeout(Long connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    /**
     * Gets ignite client address finder.
     */
    public IgniteClientAddressFinder getAddressFinder() {
        return addressFinder;
    }

    /**
     * Sets ignite client address finder.
     */
    public void setAddressFinder(IgniteClientAddressFinder addressFinder) {
        this.addressFinder = addressFinder;
    }

    /**
     * Gets SSL configuration.
     */
    public SslConfigurationProperties getSslConfiguration() {
        return sslConfiguration;
    }

    /**
     * Sets SSL configuration.
     */
    public void setSslConfiguration(SslConfigurationProperties sslConfiguration) {
        this.sslConfiguration = sslConfiguration;
    }

    /**
     * Returns {@code true} if metrics enabled.
     */
    public Boolean isMetricsEnabled() {
        return metricsEnabled;
    }

    /**
     * Sets if metrics enabled.
     */
    public void setMetricsEnabled(Boolean metricsEnabled) {
        this.metricsEnabled = metricsEnabled;
    }

    /**
     * Gets ignite client authenticator.
     */
    public IgniteClientAuthenticator getAuthenticator() {
        return authenticator;
    }

    /**
     * Sets ignite client authenticator.
     */
    public void setAuthenticator(IgniteClientAuthenticator authenticator) {
        this.authenticator = authenticator;
    }

    /**
     * Gets operation timeout.
     */
    public Long getOperationTimeout() {
        return operationTimeout;
    }

    /**
     * Sets operation timeout.
     */
    public void setOperationTimeout(Long operationTimeout) {
        this.operationTimeout = operationTimeout;
    }

    /**
     * Gets reconnect interval.
     */
    public Long getBackgroundReconnectInterval() {
        return backgroundReconnectInterval;
    }

    /**
     * Sets reconnect interval.
     */
    public void setBackgroundReconnectInterval(Long backgroundReconnectInterval) {
        this.backgroundReconnectInterval = backgroundReconnectInterval;
    }

    /**
     * Gets async continuation executor.
     */
    public Executor getAsyncContinuationExecutor() {
        return asyncContinuationExecutor;
    }

    /**
     * Sets async continuation executor.
     */
    public void setAsyncContinuationExecutor(Executor asyncContinuationExecutor) {
        this.asyncContinuationExecutor = asyncContinuationExecutor;
    }

    /**
     * Gets heartbeat interval.
     */
    public Long getHeartbeatInterval() {
        return heartbeatInterval;
    }

    /**
     * Sets heartbeat interval.
     */
    public void setHeartbeatInterval(Long heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    /**
     * Gets heartbeat timeout.
     */
    public Long getHeartbeatTimeout() {
        return heartbeatTimeout;
    }

    /**
     * Sets heartbeat timeout.
     */
    public void setHeartbeatTimeout(Long heartbeatTimeout) {
        this.heartbeatTimeout = heartbeatTimeout;
    }

    /**
     * Gets authentication properties.
     */
    public AuthenticationProperties getAuth() {
        return auth;
    }

    /**
     * Sets authentication properties.
     */
    public void setAuth(AuthenticationProperties auth) {
        this.auth = auth;
    }

    /**
     * Authentication configuration properties.
     */
    public static class AuthenticationProperties {
        private BasicAuthProperties basic;

        public BasicAuthProperties getBasic() {
            return basic;
        }

        public void setBasic(BasicAuthProperties basic) {
            this.basic = basic;
        }
    }

    /**
     * Basic authentication properties.
     */
    public static class BasicAuthProperties {
        private String username;
        private String password;

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }
}
