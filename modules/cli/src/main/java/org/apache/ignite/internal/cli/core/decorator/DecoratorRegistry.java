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

package org.apache.ignite.internal.cli.core.decorator;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.cli.decorators.DefaultDecorator;

/**
 * Registry for {@link Decorator}.
 */
public class DecoratorRegistry {
    private final Map<Class<?>, Decorator<?, TerminalOutput>> store = new HashMap<>();

    public <T> void add(Class<? extends T> clazz, Decorator<T, TerminalOutput> decorator) {
        store.put(clazz, decorator);
    }

    public void addAll(DecoratorRegistry decoratorRegistry) {
        store.putAll(decoratorRegistry.store);
    }

    // TODO: https://issues.apache.org/jira/browse/IGNITE-26175
    @SuppressWarnings("PMD.UseDiamondOperator")
    public <T> Decorator<T, TerminalOutput> getDecorator(Class<? extends T> clazz) {
        return (Decorator<T, TerminalOutput>) store.getOrDefault(clazz, new DefaultDecorator<T>());
    }
}
