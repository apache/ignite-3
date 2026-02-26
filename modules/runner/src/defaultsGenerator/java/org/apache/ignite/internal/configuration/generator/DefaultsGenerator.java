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

package org.apache.ignite.internal.configuration.generator;

import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.KeyIgnorer;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.internal.configuration.ConfigurationChanger;
import org.apache.ignite.internal.configuration.ConfigurationChanger.ConfigurationUpdateListener;
import org.apache.ignite.internal.configuration.ConfigurationModules;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.LocalFileConfigurationStorage;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidator;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidatorImpl;

/**
 * A generator of the default local configuration file.
 */
public class DefaultsGenerator {

    /**
     * Entry point to the config file generation.
     *
     * <p>The generator looks for all available configuration roots with type {@link ConfigurationType#LOCAL} in its own classpath,
     * so please make sure the classpath is properly constructed.
     *
     * @param args The first element represents the path to the config file.
     *     If the file exists and is not empty, the stored configuration will be merged with the defaults.
     *     Please note: the file will be overwritten.
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            throw new IllegalArgumentException("Please provide the path to the config file as an argument");
        }
        Path configPath = Paths.get(args[0]);

        ConfigurationChanger changer = null;
        try {
            changer = createConfigurationChanger(configPath);
            changer.start();
            changer.onDefaultsPersisted().get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to generate defaults file."
                    + "Please make sure that the classloader for loading services is correct.", e);
        } finally {
            if (changer != null) {
                changer.stop();
            }
        }
    }

    /**
     * This uses fragments of cluster initialization from {@code IgniteImpl} class to set up local configuration framework.
     */
    private static ConfigurationChanger createConfigurationChanger(Path configPath) {
        ConfigurationModules modules = ConfigurationModules.create(DefaultsGenerator.class.getClassLoader());

        ConfigurationTreeGenerator localConfigurationGenerator = new ConfigurationTreeGenerator(
                modules.local().rootKeys(),
                modules.local().schemaExtensions(),
                modules.local().polymorphicSchemaExtensions()
        );

        ConfigurationStorage storage = new LocalFileConfigurationStorage(
                "defaultGen", configPath, localConfigurationGenerator, modules.local());

        ConfigurationValidator configurationValidator =
                ConfigurationValidatorImpl.withDefaultValidators(localConfigurationGenerator, modules.local().validators());

        ConfigurationUpdateListener empty = (oldRoot, newRoot, storageRevision, notificationNumber) -> nullCompletedFuture();

        return new ConfigurationChanger(
                empty,
                modules.local().rootKeys(),
                storage,
                configurationValidator,
                c -> {},
                KeyIgnorer.fromDeletedPrefixes(modules.local().deletedPrefixes())
        ) {
            @Override
            public InnerNode createRootNode(RootKey<?, ?, ?> rootKey) {
                return localConfigurationGenerator.instantiateNode(rootKey.schemaClass());
            }
        };
    }
}
