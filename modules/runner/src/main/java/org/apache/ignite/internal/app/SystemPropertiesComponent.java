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

package org.apache.ignite.internal.app;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.utils.SystemDistributedConfigurationPropertyHolder;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.tostring.IgniteToStringBuilder;
import org.apache.ignite.internal.tostring.SensitiveDataLoggingPolicy;

/**
 * System properties initialization.
 */
class SystemPropertiesComponent implements IgniteComponent {

    /**
     * Sensitive data logging option. See {@link IgniteToStringBuilder#setSensitiveDataPolicy(SensitiveDataLoggingPolicy)}}. 
     */
    private static final String SENSITIVE_DATA_LOGGING = "sensitiveDataLogging";

    private final SystemDistributedConfigurationPropertyHolder<SensitiveDataLoggingPolicy> sensitiveDataLogging;

    SystemPropertiesComponent(SystemDistributedConfiguration configuration) {
        this.sensitiveDataLogging = new SystemDistributedConfigurationPropertyHolder<>(
                configuration,
                (value, revision) -> IgniteToStringBuilder.setSensitiveDataPolicy(value),
                SENSITIVE_DATA_LOGGING,
                SensitiveDataLoggingPolicy.HASH,
                (v) -> SensitiveDataLoggingPolicy.valueOf(v.toUpperCase())
        );
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        sensitiveDataLogging.init();

        SensitiveDataLoggingPolicy policy = sensitiveDataLogging.currentValue();
        IgniteToStringBuilder.setSensitiveDataPolicy(policy);

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        return CompletableFuture.completedFuture(null);
    }
}
