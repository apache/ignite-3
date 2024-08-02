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

package org.apache.ignite.internal.failure.configuration;

import org.apache.ignite.failure.configuration.FailureProcessorConfigurationBuilder;
import org.apache.ignite.failure.handlers.configuration.NoOpFailureHandlerBuilder;
import org.apache.ignite.failure.handlers.configuration.StopNodeFailureHandlerBuilder;
import org.apache.ignite.failure.handlers.configuration.StopNodeOrHaltFailureHandlerBuilder;
import org.apache.ignite.internal.failure.handlers.configuration.FailureHandlerBuilderImpl;
import org.apache.ignite.internal.failure.handlers.configuration.NoOpFailureHandlerBuilderImpl;
import org.apache.ignite.internal.failure.handlers.configuration.StopNodeFailureHandlerBuilderImpl;
import org.apache.ignite.internal.failure.handlers.configuration.StopNodeOrHaltFailureHandlerBuilderImpl;

public class FailureProcessorConfigurationBuilderImpl implements FailureProcessorConfigurationBuilder {
    private FailureHandlerBuilderImpl failureHandlerBuilder;

    public void buildToConfiguration(FailureProcessorConfiguration configuration) {
        if (failureHandlerBuilder != null) {
            failureHandlerBuilder.buildToConfiguration(configuration);
        }
    }

    @Override
    public NoOpFailureHandlerBuilder withNoOpFailureHandler() {
        NoOpFailureHandlerBuilderImpl builder = new NoOpFailureHandlerBuilderImpl();
        failureHandlerBuilder = builder;
        return builder;
    }

    @Override
    public StopNodeFailureHandlerBuilder withStopNodeFailureHandler() {
        StopNodeFailureHandlerBuilderImpl builder = new StopNodeFailureHandlerBuilderImpl();
        failureHandlerBuilder = builder;
        return builder;
    }

    @Override
    public StopNodeOrHaltFailureHandlerBuilder withStopNodeOrHaltFailureHandler() {
        StopNodeOrHaltFailureHandlerBuilderImpl builder = new StopNodeOrHaltFailureHandlerBuilderImpl();
        failureHandlerBuilder = builder;
        return builder;
    }
}
