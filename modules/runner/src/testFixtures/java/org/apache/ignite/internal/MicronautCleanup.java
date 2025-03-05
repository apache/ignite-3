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

package org.apache.ignite.internal;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.testframework.IgniteTestUtils;

/**
 * Utils allowing to cleanup after Micronaut instances.
 */
class MicronautCleanup {
    private static final Function<Thread, Runnable> getUnderlyingRunnable;

    static {
        List<String> fields = Arrays.stream(Thread.class.getDeclaredFields())
                .map(Field::getName)
                .collect(Collectors.toList());

        if (fields.contains("target")) { // Presence of 'target' field means that we're running Java 11-17
            getUnderlyingRunnable = hook -> IgniteTestUtils.getFieldValue(hook, Thread.class, "target");

        } else if (fields.contains("holder")) { // Presence of 'holder' field means that we're running Java 21+
            getUnderlyingRunnable = hook -> {
                Object holder = IgniteTestUtils.getFieldValue(hook, Thread.class, "holder");
                return IgniteTestUtils.getFieldValue(holder, "task");
            };
        } else {
            throw new RuntimeException("Cannot determine the underlying Runnable for the Thread class");
        }
    }

    /**
     * Removes Micronaut shutdown hooks.
     *
     * <p>Micronaut adds a shutdown hook of its own, but it's not designed to remove it. This causes 2Mb+ of heap to leak after each
     * Micronaut instance (and we have one per Ignite). This method allows to reclaim this memory in tests where we create a lot
     * of Ignite instances.
     */
    static void removeShutdownHooks() {
        Class<?> shutdownHooksClass;
        try {
            shutdownHooksClass = Class.forName("java.lang.ApplicationShutdownHooks");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        Map<Thread, Thread> hooks = IgniteTestUtils.getFieldValue(null, shutdownHooksClass, "hooks");

        Set<Thread> hooksToRemove = new HashSet<>();

        // Synchronizing because this is how ApplicationShutdownHooks acts: it accesses the hooks map in a static synchronized method.
        synchronized (shutdownHooksClass) {
            for (Thread hook : hooks.values()) {
                if (isMicronautHook(hook)) {
                    hooksToRemove.add(hook);
                }
            }
        }

        removeHooksSafely(hooksToRemove);
    }

    private static boolean isMicronautHook(Thread hook) {
        Runnable target = getUnderlyingRunnable.apply(hook);

        return target != null && target.getClass().toString().contains("Micronaut");
    }

    private static void removeHooksSafely(Set<Thread> hooksToRemove) {
        for (Thread hook : hooksToRemove) {
            try {
                Runtime.getRuntime().removeShutdownHook(hook);
            } catch (IllegalStateException ignored) {
                // Shutdown is already in progress, just ignore this.
            }
        }
    }
}
