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

import org.apache.ignite.client.IgniteClient;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * {@link EnableAutoConfiguration Auto-configuration} for Apache Ignite 3 client.
 */
@AutoConfiguration
@ConditionalOnClass(IgniteClient.class)
@EnableConfigurationProperties(IgniteClientProperties.class)
public class IgniteClientAutoConfiguration {

    @ConditionalOnMissingBean
    @Bean
    public IgniteClientPropertiesCustomizer customizer() {
        return cfg -> { /* no-op */ };
    }

    /**
     * Creates Ignite client.
     *
     * @param config Ignite configuration.
     * @param clientCustomizer ignite client configuration customizer.
     */
    @Bean
    public IgniteClient createIgniteClient(IgniteClientProperties config, IgniteClientPropertiesCustomizer clientCustomizer) {

        clientCustomizer.accept(config);

        IgniteClient.Builder builder = IgniteClient.builder();
        if (config.getAddresses() != null) {
            builder.addresses(config.getAddresses());
        }

        if (config.getConnectTimeout() != null) {
            builder.connectTimeout(config.getConnectTimeout());
        }

        if (config.isMetricsEnabled() != null) {
            builder.metricsEnabled(config.isMetricsEnabled());
        }

        if (config.getSslConfiguration() != null) {
            SslConfigurationProperties sslConfig = config.getSslConfiguration();
            if (sslConfig.enabled()) {
                builder.ssl(sslConfig);
            }
        }

        if (config.getReconnectThrottlingRetries() != null) {
            builder.reconnectThrottlingRetries(config.getReconnectThrottlingRetries());
        }

        if (config.getReconnectThrottlingPeriod() != null) {
            builder.reconnectThrottlingPeriod(config.getReconnectThrottlingPeriod());
        }

        if (config.getReconnectInterval() != null) {
            builder.reconnectInterval(config.getReconnectInterval());
        }

        if (config.getHeartbeatInterval() != null) {
            builder.heartbeatInterval(config.getHeartbeatInterval());
        }

        if (config.getHeartbeatTimeout() != null) {
            builder.heartbeatTimeout(config.getHeartbeatTimeout());
        }

        if (config.getOperationTimeout() != null) {
            builder.operationTimeout(config.getOperationTimeout());
        }

        return builder.build();
    }
}
