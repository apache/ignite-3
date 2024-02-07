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

package org.apache.ignite.internal.network.serialization;

import java.util.ServiceLoader;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper around a {@link ServiceLoader} to load {@link MessageSerializationRegistryInitializer} implementations.
 */
public class SerializationRegistryServiceLoader {
    @Nullable
    private final ClassLoader classLoader;

    public SerializationRegistryServiceLoader(@Nullable ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    /**
     * Loads all accessible {@link MessageSerializationRegistryInitializer} implementations and registers them in the provided
     * {@code registry}.
     */
    public void registerSerializationFactories(MessageSerializationRegistry registry) {
        ServiceLoader<MessageSerializationRegistryInitializer> loader =
                ServiceLoader.load(MessageSerializationRegistryInitializer.class, classLoader);

        for (MessageSerializationRegistryInitializer initializer : loader) {
            initializer.registerFactories(registry);
        }
    }
}
