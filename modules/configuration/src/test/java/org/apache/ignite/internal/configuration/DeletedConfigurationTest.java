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
import static org.apache.ignite.internal.configuration.DeletedConfigurationTest.NestedListInstanceConfigurationSchema.NAMED_LIST_NAME;
import static org.apache.ignite.internal.configuration.hocon.HoconConverter.hoconSource;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import com.typesafe.config.ConfigFactory;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.KeyIgnorer;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.WriteEntryImpl;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class DeletedConfigurationTest extends BaseIgniteAbstractTest {
    ConfigurationModule configurationModule = new ConfigurationModule() {
        @Override
        public ConfigurationType type() {
            return LOCAL;
        }

        @Override
        public Collection<String> deletedPrefixes() {
            return Set.of(
                    DELETED_INDIVIDUAL_PROPERTY,
                    DELETED_PROPERTY_MATCHING_PREFIX,
                    DELETED_SUBTREE,
                    SUBTREE_NAMED_LIST,
                    NESTED_NAMED_LISTS
            );
        }
    };

    private static final String DELETED_INDIVIDUAL_PROPERTY = "key.individual_property";
    private static final String DELETED_SUBTREE = "key.subtree";
    private static final String SUBTREE_NAMED_LIST = "key.list.*.subtree";
    private static final String NESTED_NAMED_LISTS = "key.list." + NAMED_LIST_NAME + ".*";
    private static final String DELETED_PROPERTY_MATCHING_PREFIX = "key.some";

    private static final String VALID_KEY = "key.someValue";

    private static final ConfigurationTreeGenerator GENERATOR = new ConfigurationTreeGenerator(
            Set.of(DeletedTestConfiguration.KEY),
            Set.of(),
            Set.of(NestedListInstanceConfigurationSchema.class)
    );

    private final TestConfigurationStorage storage = new TestConfigurationStorage(LOCAL);

    private ConfigurationChanger changer;

    @AfterAll
    public static void afterAll() {
        GENERATOR.close();
    }

    @AfterEach
    void tearDown() {
        storage.close();
        changer.stop();
    }

    @ParameterizedTest
    @MethodSource("deletedPropertiesProvider")
    public void testPropertyDeleted(Map<String, String> deletedProperties) {
        Map<String, String> fullConfig = new HashMap<>(deletedProperties);

        fullConfig.put(VALID_KEY, "value");

        changer = createChanger(DeletedTestConfiguration.KEY);

        CompletableFuture<Boolean> write = storage.write(new WriteEntryImpl(fullConfig, 0));
        assertThat(write, willCompleteSuccessfully());

        assertDoesNotThrow(changer::start);

        CompletableFuture<Map<String, ? extends Serializable>> readFromStorage = storage.readAllLatest("");
        Map<String, ? extends Serializable> storageData = readFromStorage.join();

        assertThat(storageData.size(), is(1));

        assertThat(storageData.get(VALID_KEY), Matchers.equalTo("value"));
    }

    @ParameterizedTest
    @MethodSource("hoconConfigProvider")
    public void testUserAddsDeletedValue(String hoconConfig) {
        changer = createChanger(DeletedTestConfiguration.KEY);

        changer.start();

        String config = hoconConfig + ", " + VALID_KEY + " = \"new_value\"";
        KeyIgnorer keyIgnorer = KeyIgnorer.fromDeletedPrefixes(configurationModule.deletedPrefixes());

        ConfigurationSource change = hoconSource(ConfigFactory.parseString(config).root(), keyIgnorer);

        assertThat(changer.change(change), willCompleteSuccessfully());

        assertThat(storage.readLatest(VALID_KEY), willBe("new_value"));
    }

    private static Stream<String> hoconConfigProvider() {
        return Stream.of(
                DELETED_INDIVIDUAL_PROPERTY + " = \"deleted\"",
                DELETED_SUBTREE + ".property = \"deleted\"",
                SUBTREE_NAMED_LIST.replace("*", NAMED_LIST_NAME) + ".property = \"deleted\"",
                NESTED_NAMED_LISTS.replace("*", NAMED_LIST_NAME) + ".property = \"deleted\""

        );
    }

    private static Stream<Map<String, ? extends Serializable>> deletedPropertiesProvider() {
        return Stream.of(
                Map.of(DELETED_INDIVIDUAL_PROPERTY, "deleted"),
                Map.of(DELETED_PROPERTY_MATCHING_PREFIX, "deleted"),
                Map.of(DELETED_SUBTREE + ".property", "deleted"),
                Map.of(SUBTREE_NAMED_LIST.replace("*", NAMED_LIST_NAME) + ".property", "deleted"),
                Map.of(NESTED_NAMED_LISTS.replace("*", NAMED_LIST_NAME) + ".property", "deleted")
        );
    }

    private ConfigurationChanger createChanger(RootKey<?, ?, ?> rootKey) {
        return new TestConfigurationChanger(
                List.of(rootKey),
                storage,
                GENERATOR,
                new TestConfigurationValidator(),
                changer -> {},
                KeyIgnorer.fromDeletedPrefixes(configurationModule.deletedPrefixes())
        );
    }

    @ConfigurationRoot(rootName = "key", type = LOCAL)
    public static class DeletedTestConfigurationSchema {
        @Value(hasDefault = true)
        public String someValue = "default";

        @NamedConfigValue
        public NestedListConfigurationSchema list;
    }

    @PolymorphicConfig
    public static class NestedListConfigurationSchema {
        @PolymorphicId(hasDefault = true)
        public String name = NAMED_LIST_NAME;
    }

    @PolymorphicConfigInstance(NAMED_LIST_NAME)
    public static class NestedListInstanceConfigurationSchema extends NestedListConfigurationSchema {
        static final String NAMED_LIST_NAME = "named-list";
    }
}
