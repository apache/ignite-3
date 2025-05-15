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

import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.configuration.hocon.HoconConverter.hoconSource;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;

import com.typesafe.config.ConfigFactory;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.InjectedName;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.configuration.annotation.PublicName;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RenamedConfigurationTest extends BaseIgniteAbstractTest {
    private static final String OLD_VALUE_NAME = "oldName";
    private static final String OLD_INNER_NAME = "oldInnerName";
    private static final String OLD_LIST_NAME = "listOldName";
    private static final String POLYMORPHIC_TYPE = "polymorphicType";
    private static final String OLD_POLYMORPHIC_NAME = "oldPolymorphicName";

    private static final ConfigurationTreeGenerator OLD_GENERATOR = new ConfigurationTreeGenerator(
            Set.of(RenamedTestOldConfiguration.KEY),
            Set.of(),
            Set.of(RenamedPolymorphicInstanceOldConfigurationSchema.class)
    );

    private static final ConfigurationTreeGenerator NEW_GENERATOR = new ConfigurationTreeGenerator(
            Set.of(RenamedTestNewConfiguration.KEY),
            Set.of(),
            Set.of(RenamedPolymorphicInstanceNewConfigurationSchema.class)
    );
    public static final String OLD_DEFAULT = "oldDefault";

    private final TestConfigurationStorage storage = new TestConfigurationStorage(LOCAL);

    private ConfigurationRegistry registry;

    @AfterAll
    public static void afterAll() {
        OLD_GENERATOR.close();
    }

    @BeforeEach
    void setUp() {
        registry = startRegistry(RenamedTestOldConfiguration.KEY, OLD_GENERATOR);

        stopRegistry(registry);

        registry = startRegistry(RenamedTestNewConfiguration.KEY, NEW_GENERATOR);
    }

    @AfterEach
    void tearDown() {
        stopRegistry(registry);
    }

    @Test
    public void testLegacyNameIsRecognisedOnStartup() {
        assertThat(
                registry.getConfiguration(RenamedTestNewConfiguration.KEY).newInnerName().newName().value(),
                Matchers.equalTo(OLD_DEFAULT)
        );

        assertThat(storage.readLatest(String.format("key.%s.%s", OLD_INNER_NAME, OLD_VALUE_NAME)), willBe(nullValue()));
    }

    @Test
    public void testLegacyNameIsRecognisedOnUpdate() {
        String updatedValue = "updatedValue";
        String updatedConfig = String.format("key.%s.%s = \"%s\"", OLD_INNER_NAME, OLD_VALUE_NAME, updatedValue);

        updateConfig(registry, updatedConfig);

        assertThat(registry.getConfiguration(RenamedTestNewConfiguration.KEY).newInnerName().newName().value(), Matchers.equalTo(updatedValue));
        assertThat(storage.readLatest(String.format("key.%s.%s", OLD_INNER_NAME, OLD_VALUE_NAME)), willBe(nullValue()));

        stopRegistry(registry);
        startRegistry(RenamedTestNewConfiguration.KEY, NEW_GENERATOR);
        assertThat(registry.getConfiguration(RenamedTestNewConfiguration.KEY).newInnerName().newName().value(), Matchers.equalTo(updatedValue));
    }

    @Test
    public void testNamedListLegacyNameIsRecognisedOnUpdate() {
        registry = startRegistry(RenamedTestNewConfiguration.KEY, NEW_GENERATOR);

        String instanceName = "listInstance";
        String newValue = "newValue";

        String updatedConfig = String.format("key.%s.%s.%s=\"%s\"", OLD_LIST_NAME, instanceName, OLD_VALUE_NAME, newValue);
        updateConfig(registry, updatedConfig);

        assertThat(
                registry.getConfiguration(RenamedTestNewConfiguration.KEY).newListName().get(instanceName).newName().value(),
                Matchers.equalTo(newValue)
        );
    }

    @Test
    public void testPolymorphicLegacyNameIsRecognisedOnUpdate() {
        registry = startRegistry(RenamedTestNewConfiguration.KEY, NEW_GENERATOR);

        String newValue = "newValue";
        String updatedConfig = String.format("key.%s.%s.%s=\"%s\"", OLD_POLYMORPHIC_NAME, POLYMORPHIC_TYPE, OLD_VALUE_NAME, newValue);
        updateConfig(registry, updatedConfig);

        assertThat(
                registry.getConfiguration(RenamedTestNewConfiguration.KEY).newPolymorphicName().get(POLYMORPHIC_TYPE).newName().value(),
                Matchers.equalTo(newValue)
        );
    }

    private static void updateConfig(ConfigurationRegistry registry, String updatedConfig) {
        CompletableFuture<Void> change = registry.change(hoconSource(ConfigFactory.parseString(updatedConfig).root()));
        assertThat(change, willCompleteSuccessfully());
    }

    private ConfigurationRegistry startRegistry(RootKey<?, ?> rootKey, ConfigurationTreeGenerator generator) {
        var registry = new ConfigurationRegistry(Set.of(rootKey), storage, generator, new TestConfigurationValidator());

        assertThat(registry.startAsync(new ComponentContext()), willCompleteSuccessfully());
        assertThat(registry.onDefaultsPersisted(), willCompleteSuccessfully());

        return registry;
    }

    private void stopRegistry(ConfigurationRegistry registry) {
        assertThat(registry.stopAsync(), willCompleteSuccessfully());
        // Removes registry update listener.
        storage.close();
    }

    @ConfigurationRoot(rootName = "key", type = LOCAL)
    public static class RenamedTestOldConfigurationSchema {
        @ConfigValue
        @PublicName(OLD_INNER_NAME)
        public RenamedLeafOldConfigurationSchema oldInnerName;

        @NamedConfigValue
        @PublicName(OLD_LIST_NAME)
        public RenamedLeafOldConfigurationSchema oldListName;

        @NamedConfigValue
        @PublicName(OLD_POLYMORPHIC_NAME)
        public RenamedPolymorphicOldConfigurationSchema oldPolymorphicName;
    }

    @ConfigurationRoot(rootName = "key", type = LOCAL)
    public static class RenamedTestNewConfigurationSchema {
        @ConfigValue
        @PublicName(legacyNames = OLD_INNER_NAME)
        public RenamedLeafNewConfigurationSchema newInnerName;

        @NamedConfigValue
        @PublicName(legacyNames = {OLD_LIST_NAME})
        public RenamedLeafNewConfigurationSchema newListName;

        @NamedConfigValue
        @PublicName(legacyNames = OLD_POLYMORPHIC_NAME)
        public RenamedPolymorphicNewConfigurationSchema newPolymorphicName;
    }

    @Config
    public static class RenamedLeafOldConfigurationSchema {
        @Value(hasDefault = true)
        @PublicName(OLD_VALUE_NAME)
        public String oldName = OLD_DEFAULT;
    }

    @Config
    public static class RenamedLeafNewConfigurationSchema {
        @Value(hasDefault = true)
        @PublicName(legacyNames = {OLD_VALUE_NAME})
        public String newName = "newDefault";
    }

    @PolymorphicConfig
    public static class RenamedPolymorphicOldConfigurationSchema {
        @PolymorphicId(hasDefault = true)
        public String type = POLYMORPHIC_TYPE;

        @InjectedName
        public String name;

        @Value(hasDefault = true)
        @PublicName(OLD_VALUE_NAME)
        public String oldName = OLD_DEFAULT;
    }

    @PolymorphicConfigInstance(POLYMORPHIC_TYPE)
    public static class RenamedPolymorphicInstanceOldConfigurationSchema extends RenamedPolymorphicOldConfigurationSchema {
    }

    @PolymorphicConfig
    public static class RenamedPolymorphicNewConfigurationSchema {
        @PolymorphicId(hasDefault = true)
        public String type = POLYMORPHIC_TYPE;

        @InjectedName
        public String name;

        @Value(hasDefault = true)
        @PublicName(legacyNames = OLD_VALUE_NAME)
        public String newName = "newDefault";
    }

    @PolymorphicConfigInstance(POLYMORPHIC_TYPE)
    public static class RenamedPolymorphicInstanceNewConfigurationSchema extends RenamedPolymorphicNewConfigurationSchema{
    }
}
