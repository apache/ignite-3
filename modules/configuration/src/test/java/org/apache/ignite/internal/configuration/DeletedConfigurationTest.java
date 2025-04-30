package org.apache.ignite.internal.configuration;

import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
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
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class DeletedConfigurationTest {
    private static final String DELETED_INDIVIDUAL_PROPERTY = "individual.property";
    private static final String DELETED_SUBTREE = "subtree";
    private static final String SUBTREE_NAMED_LIST = "subtree.named.*.list";
    private static final String NESTED_NAMED_LISTS = "nested.*.named.*.list";

    private static final Collection<String> DELETED_KEYS = List.of(
            DELETED_INDIVIDUAL_PROPERTY,
            DELETED_SUBTREE,
            SUBTREE_NAMED_LIST,
            NESTED_NAMED_LISTS
    );

    private static final String VALID_KEY = "key.someValue";

    @ConfigurationRoot(rootName = "key", type = LOCAL)
    public static class DeletedTestConfigurationSchema {
        @Value(hasDefault = true)
        public String someValue = "default";
    }

    private static final ConfigurationTreeGenerator GENERATOR = new ConfigurationTreeGenerator(DeletedTestConfiguration.KEY);

    private final TestConfigurationStorage storage = new TestConfigurationStorage(LOCAL);

    @AfterAll
    public static void afterAll() {
        GENERATOR.close();
    }

    @ParameterizedTest
    @MethodSource("deletedPropertiesProvider")
    public void testPropertyDeleted(Map<String, String> deletedProperties) {
        Map<String, String> fullConfig = new HashMap<>(deletedProperties);

        fullConfig.put(VALID_KEY, "value");

        ConfigurationChanger changer = createChanger(DeletedTestConfiguration.KEY);

        CompletableFuture<Boolean> write = storage.write(fullConfig, 0);
        assertThat(write, willCompleteSuccessfully());

        assertDoesNotThrow(changer::start);

        CompletableFuture<Map<String, ? extends Serializable>> readFromStorage = storage.readAllLatest("");
        Map<String, ? extends Serializable> storageData = readFromStorage.join();

        assertThat(storageData.size(), is(1));

        assertThat(storageData.get(VALID_KEY), Matchers.equalTo("value"));
    }

    @Test
    public void testUserAddsDeletedValue() {
        ConfigurationChanger changer = createChanger(DeletedTestConfiguration.KEY);

        changer.start();

        String config = DELETED_INDIVIDUAL_PROPERTY + " = \"value\", " + VALID_KEY + " = \"3\"";

        ConfigurationSource change = hoconSource(ConfigFactory.parseString(config).root());

        assertThat(changer.change(change), willCompleteSuccessfully());

        assertThat(storage.readLatest(VALID_KEY), willBe(3));
    }

    private static Stream<Map<String, ? extends Serializable>> deletedPropertiesProvider() {
        return Stream.of(
                Map.of(DELETED_INDIVIDUAL_PROPERTY, "deleted"),
                Map.of(DELETED_SUBTREE + ".property", "deleted", DELETED_SUBTREE + ".property2", "deleted"),
                Map.of(SUBTREE_NAMED_LIST.replace("*", "name") + ".property", "deleted"),
                Map.of(NESTED_NAMED_LISTS.replace("*", "name") + ".property", "deleted")
        );
    }

    private ConfigurationChanger createChanger(RootKey<?, ?> rootKey) {
        return new TestConfigurationChanger(
                List.of(rootKey),
                storage,
                GENERATOR,
                new TestConfigurationValidator(),
                changer -> {},
                DELETED_KEYS
        );
    }
}
