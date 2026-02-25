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

import static java.util.Map.entry;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.configuration.hocon.HoconConverter.hoconSource;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.typesafe.config.ConfigFactory;
import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
import org.apache.ignite.internal.configuration.storage.ReadEntry;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RenamedConfigurationTest extends BaseIgniteAbstractTest {

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

        String updatedConfig = "key.listOldName.listInstance.oldName = oldValue, "
                + "key.oldPolymorphicName.polymorphicType.oldName = oldValue";

        assertThat(registry.change(hoconSource(ConfigFactory.parseString(updatedConfig).root())), willCompleteSuccessfully());

        stopRegistry(registry);

        registry = startRegistry(RenamedTestNewConfiguration.KEY, NEW_GENERATOR);
    }

    @AfterEach
    void tearDown() {
        stopRegistry(registry);
    }

    @Test
    public void testLegacyNameIsRecognisedOnStartup() {
        // Default value was set when registry was started with old configuration.
        assertThat(
                registry.getConfiguration(RenamedTestNewConfiguration.KEY).newInnerName().newName().value(),
                equalTo(OLD_DEFAULT)
        );
        assertThat(
                registry.getConfiguration(RenamedTestNewConfiguration.KEY).newInnerName().name().value(),
                equalTo(OLD_DEFAULT)
        );

        assertThat(storage.readLatest("key.oldInnerName.oldName"), willBe(nullValue()));
        assertThat(storage.readLatest("key.oldInnerName.name"), willBe(nullValue()));
    }

    @Test
    public void testLegacyNameIsRecognisedOnUpdate() {
        String updatedValue = "updatedValue";
        String configWithFirstLegacyName = "key.oldInnerName.oldName = " + updatedValue;
        updateConfig(registry, configWithFirstLegacyName);

        assertThat(
                registry.getConfiguration(RenamedTestNewConfiguration.KEY).newInnerName().newName().value(),
                equalTo(updatedValue)
        );
        assertThat(storage.readLatest("key.oldInnerName.oldName"), willBe(nullValue()));

        String secondUpdatedValue = "secondUpdatedValue";
        String configWithSecondLegacyName = "key.secondOldInnerName.oldName = " + secondUpdatedValue;

        updateConfig(registry, configWithSecondLegacyName);

        assertThat(
                registry.getConfiguration(RenamedTestNewConfiguration.KEY).newInnerName().newName().value(),
                equalTo(secondUpdatedValue)
        );
        assertThat(storage.readLatest("key.secondOldInnerName.oldName"), willBe(nullValue()));
    }

    @Test
    public void testNewValuePersistsAfterRestart() {
        String updatedValue = "updatedValue";
        String updatedConfig = "key.oldInnerName.oldName = " + updatedValue;
        updateConfig(registry, updatedConfig);

        assertThat(
                registry.getConfiguration(RenamedTestNewConfiguration.KEY).newInnerName().newName().value(),
                equalTo(updatedValue)
        );

        stopRegistry(registry);
        registry = startRegistry(RenamedTestNewConfiguration.KEY, NEW_GENERATOR);

        assertThat(storage.readLatest("key.oldInnerName.oldName"), willBe(nullValue()));

        assertThat(
                registry.getConfiguration(RenamedTestNewConfiguration.KEY).newInnerName().newName().value(),
                equalTo(updatedValue)
        );
    }

    @Test
    public void testNamedListLegacyNameIsRecognisedOnStartup() {
        assertThat(
                registry.getConfiguration(RenamedTestNewConfiguration.KEY).newListName().get("listInstance").newName().value(),
                equalTo("oldValue")
        );

        validateStorageContent(
                entry("key.newInnerName.name", "oldDefault"),
                entry("key.newInnerName.newName", "oldDefault"),
                entry("key.newListName.$listId.<name>", "listInstance"),
                entry("key.newListName.$listId.<order>", 0),
                entry("key.newListName.$listId.name", "oldDefault"),
                entry("key.newListName.$listId.newName", "oldValue"),
                entry("key.newListName.<ids>.listInstance", "$listId"),
                entry("key.newPolymorphicName.<ids>.polymorphicType", "$polymorphicId"),
                entry("key.newPolymorphicName.$polymorphicId.<name>", "polymorphicType"),
                entry("key.newPolymorphicName.$polymorphicId.<order>", 0),
                entry("key.newPolymorphicName.$polymorphicId.newName", "oldValue"),
                entry("key.newPolymorphicName.$polymorphicId.type", "polymorphicType")
        );
    }

    @Test
    public void testNamedListLegacyNameIsRecognisedOnUpdate() {
        String newValue = "newValue";

        String updatedConfig = "key.listOldName.listInstance.oldName = " + newValue;
        updateConfig(registry, updatedConfig);

        assertThat(
                registry.getConfiguration(RenamedTestNewConfiguration.KEY).newListName().get("listInstance").newName().value(),
                equalTo(newValue)
        );

        validateStorageContent(
                entry("key.newInnerName.name", "oldDefault"),
                entry("key.newInnerName.newName", "oldDefault"),
                entry("key.newListName.$listId.<name>", "listInstance"),
                entry("key.newListName.$listId.<order>", 0),
                entry("key.newListName.$listId.name", "oldDefault"),
                entry("key.newListName.$listId.newName", newValue),
                entry("key.newListName.<ids>.listInstance", "$listId"),
                entry("key.newPolymorphicName.<ids>.polymorphicType", "$polymorphicId"),
                entry("key.newPolymorphicName.$polymorphicId.<name>", "polymorphicType"),
                entry("key.newPolymorphicName.$polymorphicId.<order>", 0),
                entry("key.newPolymorphicName.$polymorphicId.newName", "oldValue"),
                entry("key.newPolymorphicName.$polymorphicId.type", "polymorphicType")
        );
    }

    @Test
    public void testPolymorphicLegacyNameIsRecognisedOnStartup() {
        assertThat(
                registry.getConfiguration(RenamedTestNewConfiguration.KEY).newPolymorphicName().get("polymorphicType").newName().value(),
                equalTo("oldValue")
        );

        validateStorageContent(
                entry("key.newInnerName.name", "oldDefault"),
                entry("key.newInnerName.newName", "oldDefault"),
                entry("key.newListName.$listId.<name>", "listInstance"),
                entry("key.newListName.$listId.<order>", 0),
                entry("key.newListName.$listId.name", "oldDefault"),
                entry("key.newListName.$listId.newName", "oldValue"),
                entry("key.newListName.<ids>.listInstance", "$listId"),
                entry("key.newPolymorphicName.<ids>.polymorphicType", "$polymorphicId"),
                entry("key.newPolymorphicName.$polymorphicId.<name>", "polymorphicType"),
                entry("key.newPolymorphicName.$polymorphicId.<order>", 0),
                entry("key.newPolymorphicName.$polymorphicId.newName", "oldValue"),
                entry("key.newPolymorphicName.$polymorphicId.type", "polymorphicType")
        );
    }

    @Test
    public void testPolymorphicLegacyNameIsRecognisedOnUpdate() {
        String newValue = "newValue";
        String updatedConfig = "key.oldPolymorphicName.polymorphicType.oldName = " + newValue;
        updateConfig(registry, updatedConfig);

        assertThat(
                registry.getConfiguration(RenamedTestNewConfiguration.KEY).newPolymorphicName().get("polymorphicType").newName().value(),
                equalTo(newValue)
        );

        validateStorageContent(
                entry("key.newInnerName.name", "oldDefault"),
                entry("key.newInnerName.newName", "oldDefault"),
                entry("key.newListName.$listId.<name>", "listInstance"),
                entry("key.newListName.$listId.<order>", 0),
                entry("key.newListName.$listId.name", "oldDefault"),
                entry("key.newListName.$listId.newName", "oldValue"),
                entry("key.newListName.<ids>.listInstance", "$listId"),
                entry("key.newPolymorphicName.<ids>.polymorphicType", "$polymorphicId"),
                entry("key.newPolymorphicName.$polymorphicId.<name>", "polymorphicType"),
                entry("key.newPolymorphicName.$polymorphicId.<order>", 0),
                entry("key.newPolymorphicName.$polymorphicId.newName", newValue),
                entry("key.newPolymorphicName.$polymorphicId.type", "polymorphicType")
        );
    }

    /**
     * Validates that the storage content matches the expected values. Because storage contains several "dynamic" values, whose IDs are not
     * constants, we denote them as strings {@code "$listId"} and {@code "$polymorphicId"}.All occasions of these strings will be replaced
     * with real values of these identifiers.
     */
    @SafeVarargs
    private void validateStorageContent(Map.Entry<String, Serializable> ...values) {
        CompletableFuture<ReadEntry> dataFuture = storage.readDataOnRecovery();
        assertThat(dataFuture, willCompleteSuccessfully());

        RenamedTestNewView node = registry.getConfiguration(RenamedTestNewConfiguration.KEY).value();

        UUID listId = ConfigurationUtil.internalId(node.newListName(), "listInstance");
        UUID polymorphicId = ConfigurationUtil.internalId(node.newPolymorphicName(), "polymorphicType");

        Function<Entry<String, ? extends Serializable>, String> keyMapper = e ->
                e.getKey().replace("$listId", listId.toString()).replace("$polymorphicId", polymorphicId.toString());

        Function<Entry<String, ? extends Serializable>, Serializable> valueMapper = e -> {
            if ("$listId".equals(e.getValue())) {
                return listId;
            } else if ("$polymorphicId".equals(e.getValue())) {
                return polymorphicId;
            } else {
                return e.getValue();
            }
        };

        Map<String, ? extends Serializable> expectedMap = Stream.of(values).collect(Collectors.toMap(keyMapper, valueMapper));

        assertEquals(expectedMap, dataFuture.join().values());
    }

    private static void updateConfig(ConfigurationRegistry registry, String updatedConfig) {
        CompletableFuture<Void> change = registry.change(hoconSource(ConfigFactory.parseString(updatedConfig).root()));
        assertThat(change, willCompleteSuccessfully());
    }

    private ConfigurationRegistry startRegistry(RootKey<?, ?, ?> rootKey, ConfigurationTreeGenerator generator) {
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

    private ReadEntry getData() {
        CompletableFuture<ReadEntry> dataFuture = storage.readDataOnRecovery();

        assertThat(dataFuture, willCompleteSuccessfully());

        return dataFuture.join();
    }

    @ConfigurationRoot(rootName = "key", type = LOCAL)
    public static class RenamedTestOldConfigurationSchema {
        @ConfigValue
        @PublicName("oldInnerName")
        public RenamedLeafOldConfigurationSchema oldInnerName;

        @NamedConfigValue
        @PublicName("listOldName")
        public RenamedLeafOldConfigurationSchema oldListName;

        @NamedConfigValue
        @PublicName("oldPolymorphicName")
        public RenamedPolymorphicOldConfigurationSchema oldPolymorphicName;
    }

    @ConfigurationRoot(rootName = "key", type = LOCAL)
    public static class RenamedTestNewConfigurationSchema {
        @ConfigValue
        @PublicName(legacyNames = {"oldInnerName", "secondOldInnerName"})
        public RenamedLeafNewConfigurationSchema newInnerName;

        @NamedConfigValue
        @PublicName(legacyNames = {"listOldName"})
        public RenamedLeafNewConfigurationSchema newListName;

        @NamedConfigValue
        @PublicName(legacyNames = "oldPolymorphicName")
        public RenamedPolymorphicNewConfigurationSchema newPolymorphicName;
    }

    @Config
    public static class RenamedLeafOldConfigurationSchema {
        @Value(hasDefault = true)
        @PublicName("oldName")
        public String oldName = OLD_DEFAULT;

        @Value(hasDefault = true)
        public String name = OLD_DEFAULT;
    }

    @Config
    public static class RenamedLeafNewConfigurationSchema {
        @Value(hasDefault = true)
        @PublicName(legacyNames = {"oldName"})
        public String newName = "newDefault";

        @Value(hasDefault = true)
        public String name = "newDefault";
    }

    @PolymorphicConfig
    public static class RenamedPolymorphicOldConfigurationSchema {
        @PolymorphicId(hasDefault = true)
        public String type = "polymorphicType";

        @InjectedName
        public String name;

        @Value(hasDefault = true)
        @PublicName("oldName")
        public String oldName = OLD_DEFAULT;
    }

    @PolymorphicConfigInstance("polymorphicType")
    public static class RenamedPolymorphicInstanceOldConfigurationSchema extends RenamedPolymorphicOldConfigurationSchema {
    }

    @PolymorphicConfig
    public static class RenamedPolymorphicNewConfigurationSchema {
        @PolymorphicId(hasDefault = true)
        public String type = "polymorphicType";

        @InjectedName
        public String name;

        @Value(hasDefault = true)
        @PublicName(legacyNames = "oldName")
        public String newName = "newDefault";
    }

    @PolymorphicConfigInstance("polymorphicType")
    public static class RenamedPolymorphicInstanceNewConfigurationSchema extends RenamedPolymorphicNewConfigurationSchema{
    }
}
