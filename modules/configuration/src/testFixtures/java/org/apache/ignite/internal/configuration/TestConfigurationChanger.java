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

package org.apache.ignite.internal.configuration;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidator;
import org.jetbrains.annotations.Nullable;

/** Implementation of {@link ConfigurationChanger} to be used in tests. Has no support of listeners. */
public class TestConfigurationChanger extends ConfigurationChanger {
    /** Runtime implementations generator for node classes. */
    private final ConfigurationTreeGenerator generator;

    /**
     * Constructor.
     *
     * @param rootKeys                    Configuration root keys.
     * @param storage                     Configuration storage.
     * @param generator                   Runtime implementations tree generator for node classes.
     * @throws IllegalArgumentException If the configuration type of the root keys is not equal to the storage type.
     */
    public TestConfigurationChanger(
            Collection<RootKey<?, ?>> rootKeys,
            ConfigurationStorage storage,
            ConfigurationTreeGenerator generator,
            ConfigurationValidator configurationValidator
    ) {
        super(noOpListener(), rootKeys, storage, configurationValidator);

        this.generator = generator;
    }

    private static ConfigurationUpdateListener noOpListener() {
        return new ConfigurationUpdateListener() {
            @Override
            public CompletableFuture<Void> onConfigurationUpdated(@Nullable SuperRoot oldRoot, SuperRoot newRoot, long storageRevision,
                    long notificationNumber) {
                return nullCompletedFuture();
            }
        };
    }

    /** {@inheritDoc} */
    @Override
    public InnerNode createRootNode(RootKey<?, ?> rootKey) {
        return generator.createRootNode(rootKey);
    }
}
