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

package org.apache.ignite.internal.network;

import java.util.ServiceLoader;
import org.jetbrains.annotations.Nullable;

/** Class for getting {@link ChannelTypeRegistry}. */
public class ChannelTypeRegistryProvider {
    /**
     * Returns the {@link ChannelTypeRegistry} got from the {@link ServiceLoader} with {@link ChannelType} registered in each module
     * through {@link ChannelTypeModule}.
     *
     * @param classLoader Classloader to use, {@code null} if use the system class loader.
     */
    public static ChannelTypeRegistry loadByServiceLoader(@Nullable ClassLoader classLoader) {
        var channelTypeRegisterer = new ChannelTypeRegistrar();

        for (ChannelTypeModule moduleRegisterer : ServiceLoader.load(ChannelTypeModule.class, classLoader)) {
            moduleRegisterer.register(channelTypeRegisterer);
        }

        return ChannelTypeRegistry.of(channelTypeRegisterer.getAll());
    }
}
