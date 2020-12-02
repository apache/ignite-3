/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.configuration.internal.selector;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.internal.DynamicConfiguration;
import org.apache.ignite.configuration.internal.property.Modifier;

/**
 * Base selector holder.
 */
public class BaseSelectors {
    /** Map from string representation of selector to {@link SelectorHolder}. */
    private static final Map<String, SelectorHolder> selectors = new HashMap<>();

    /**
     * Get selector from selectors map by key.
     *
     * Valid formats for selector key:
     * <ul>
     *     <li>root.inner.option.field in case of static config field</li>
     *     <li>root.inner.named[name].field in case of dynamic (named) config field</li>
     * </ul>
     *
     * @param name Selector name.
     * @return Selector.
     */
    public static <A extends DynamicConfiguration<?, ?, ?>, B extends Modifier<C, D, E>, C, D, E> Selector<A, B, C, D, E> find(String name) {
        String[] splitten = name.split("\\.");
        List<String> arguments = new ArrayList<>();
        StringBuilder keyBuilder = new StringBuilder();
        for (int i = 0; i < splitten.length; i++) {
            String part = splitten[i];
            int start = part.indexOf('[');
            String methodArg = null;
            if (start != -1) {
                int end = part.indexOf(']');
                if (end != -1) {
                    methodArg = part.substring(start + 1, end);
                    part = part.substring(0, start);
                }
            }
            if (methodArg != null)
                arguments.add(methodArg);

            keyBuilder.append(part);

            if (i != splitten.length - 1)
                keyBuilder.append('.');
        }
        final String key = keyBuilder.toString();

        final SelectorHolder selector = selectors.get(key);

        if (selector == null) {
            final int lastDot = key.lastIndexOf('.');
            if (lastDot != -1) {
                String partialKey = key.substring(0, lastDot);
                final SelectorHolder partialSelector = selectors.get(partialKey);
                if (partialSelector != null) {
                    final String availableOptions = selectors.keySet().stream().filter(s -> s.startsWith(partialKey)).collect(Collectors.joining(", "));
                    throw new SelectorNotFoundException("Selector " + key + " was not found, available options are: " + availableOptions);
                }
            }
        }

        try {
            return (Selector<A, B, C, D, E>) selector.get(arguments);
        } catch (Throwable throwable) {
            throw new SelectorNotFoundException("Failed to get selector: " + throwable.getMessage(), throwable);
        }
    }

    public static void put(String key, Selector<?, ?, ?, ?, ?> selector) {
        selectors.put(key, new SelectorHolder(selector));
    }

    public static void put(String key, MethodHandle handle) {
        selectors.put(key, new SelectorHolder(handle));
    }

    private static final class SelectorHolder {

        Selector<?, ?, ?, ?, ?> selector;

        MethodHandle selectorFn;

        public SelectorHolder(Selector<?, ?, ?, ?, ?> selector) {
            this.selector = selector;
        }

        public SelectorHolder(MethodHandle selectorFn) {
            this.selectorFn = selectorFn;
        }

        Selector<?, ?, ?, ?, ?> get(List<String> arguments) throws Throwable {
            if (selector != null)
                return selector;

            return (Selector<?, ?, ?, ?, ?>) selectorFn.invokeWithArguments(arguments);
        }

    }

}
