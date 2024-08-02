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

package org.apache.ignite.internal.failure.handlers.configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.failure.handlers.configuration.StopNodeOrHaltFailureHandlerBuilder;
import org.apache.ignite.internal.failure.configuration.FailureProcessorConfiguration;

public class StopNodeOrHaltFailureHandlerBuilderImpl extends FailureHandlerBuilderImpl
        implements StopNodeOrHaltFailureHandlerBuilder {
    private final List<Consumer<StopNodeOrHaltFailureHandlerChange>> changes = new ArrayList<>();

    @Override
    public StopNodeOrHaltFailureHandlerBuilder setTryStop(boolean tryStop) {
        changes.add(change -> change.changeTryStop(tryStop));
        return this;
    }

    @Override
    public StopNodeOrHaltFailureHandlerBuilder setTimeoutMillis(long timeoutMillis) {
        changes.add(change -> change.changeTimeoutMillis(timeoutMillis));
        return this;
    }

    @Override
    public void buildToConfiguration(FailureProcessorConfiguration configuration) {
        super.buildToConfiguration(configuration);
        configuration.change(c -> c.changeHandler(handlerChange -> {
            StopNodeOrHaltFailureHandlerChange change = handlerChange.convert(StopNodeOrHaltFailureHandlerChange.class);
            changes.forEach(consumer -> consumer.accept(change));
        })).join();
    }
}
