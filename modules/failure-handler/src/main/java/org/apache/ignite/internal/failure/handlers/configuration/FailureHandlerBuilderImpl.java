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
import org.apache.ignite.failure.handlers.configuration.FailureHandlerBuilder;

public class FailureHandlerBuilderImpl implements FailureHandlerBuilder {
    private final List<Consumer<FailureHandlerChange>> changes = new ArrayList<>();

    @Override
    public FailureHandlerBuilder setIgnoredFailureTypes(String[] ignoredFailureTypes) {
        changes.add(change -> change.changeIgnoredFailureTypes(ignoredFailureTypes));
        return this;
    }

    public void buildToConfiguration(FailureHandlerConfiguration configuration) {
        configuration.change(c -> changes.forEach(consumer -> consumer.accept(c))).join();
    }
}
