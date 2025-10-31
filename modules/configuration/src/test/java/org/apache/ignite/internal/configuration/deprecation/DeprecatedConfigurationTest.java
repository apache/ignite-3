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

package org.apache.ignite.internal.configuration.deprecation;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.SuperRootChange;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.ConfigurationChanger;
import org.apache.ignite.internal.configuration.ConfigurationMigrator;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.SuperRoot;
import org.apache.ignite.internal.configuration.SuperRootChangeImpl;
import org.apache.ignite.internal.configuration.TestConfigurationChanger;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.ReadEntry;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.WriteEntryImpl;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNode;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.TraversableTreeNodeTest.ChildConfigurationSchema;
import org.apache.ignite.internal.configuration.tree.TraversableTreeNodeTest.NamedElementConfigurationSchema;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for configuration schemas with {@link Deprecated} properties.
 */
@ExtendWith(MockitoExtension.class)
public class DeprecatedConfigurationTest extends BaseIgniteAbstractTest {
    private static final ConfigurationType TEST_CONFIGURATION_TYPE = ConfigurationType.LOCAL;

    private ConfigurationStorage storage;

    /**
     * Argument captor for {@link ConfigurationStorage#write(Map, long)}.
     */
    @Captor
    private ArgumentCaptor<Map<String, Serializable>> lastWriteCapture;

    /**
     * Configuration root schema without any deprecations.
     */
    @ConfigurationRoot(rootName = "root", type = ConfigurationType.LOCAL)
    public static class BeforeDeprecationConfigurationSchema {
        @Value(hasDefault = true)
        public int intValue = 10;

        @ConfigValue
        public ChildConfigurationSchema child;

        @NamedConfigValue
        public NamedElementConfigurationSchema list;
    }

    /**
     * Configuration root schema with deprecated primitive configuration field.
     */
    @ConfigurationRoot(rootName = "root", type = ConfigurationType.LOCAL)
    public static class DeprecatedValueConfigurationSchema {
        @Deprecated
        @Value(hasDefault = true)
        public int intValue = 10;

        @ConfigValue
        public ChildConfigurationSchema child;

        @NamedConfigValue
        public NamedElementConfigurationSchema list;
    }

    /**
     * Configuration root schema with deprecated child configuration field.
     */
    @ConfigurationRoot(rootName = "root", type = ConfigurationType.LOCAL)
    public static class DeprecatedChildConfigurationSchema {
        @Value(hasDefault = true)
        public int intValue = 10;

        @Deprecated
        @ConfigValue
        public ChildConfigurationSchema child;

        @NamedConfigValue
        public NamedElementConfigurationSchema list;
    }

    /**
     * Configuration root schema with deprecated named list configuration field.
     */
    @ConfigurationRoot(rootName = "root", type = ConfigurationType.LOCAL)
    public static class DeprecatedNamedListConfigurationSchema {
        @Value(hasDefault = true)
        public int intValue = 10;

        @ConfigValue
        public ChildConfigurationSchema child;

        @Deprecated
        @NamedConfigValue
        public NamedElementConfigurationSchema list;
    }

    /**
     * Configuration root schema with deprecated field inside named list configuration.
     */
    @ConfigurationRoot(rootName = "root", type = ConfigurationType.LOCAL)
    public static class DeprecatedFieldInNamedListConfigurationSchema {
        @Value(hasDefault = true)
        public int intValue = 10;

        @ConfigValue
        public ChildConfigurationSchema child;

        @NamedConfigValue
        public DeprecatedElementConfigurationSchema list;
    }

    /**
     * Configuration schema with deprecated field inside.
     */
    @Config
    public static class DeprecatedElementConfigurationSchema {
        @Deprecated
        @Value
        public String strCfg;
    }

    @BeforeEach
    void setUp() {
        storage = spy(new TestConfigurationStorage(TEST_CONFIGURATION_TYPE));

        when(storage.write(new WriteEntryImpl(lastWriteCapture.capture(), anyLong()))).thenAnswer(InvocationOnMock::callRealMethod);
    }

    @AfterEach
    void tearDown() {
        storage.close();
    }

    /**
     * Common assertions for the initial state of the test. In all other tests we rely on invariants stated here.
     */
    @Test
    void testInitialState() {
        withConfigurationChanger(BeforeDeprecationConfiguration.KEY, true,  change -> {}, changer -> {
            assertEquals(
                    Map.of(
                            "root.intValue", 10,
                            "root.child.my-int-cfg", 99
                    ),
                    lastWriteCapture.getValue()
            );

            ReadEntry data = getData();

            assertEquals(1, data.changeId());
            assertEquals(
                    Map.of(
                            "root.intValue", 10,
                            "root.child.my-int-cfg", 99
                    ),
                    data.values()
            );
        });
    }

    @Test
    void testDeprecatedPrimitiveConfiguration() {
        withConfigurationChanger(BeforeDeprecationConfiguration.KEY, true, change -> {}, changer -> {});

        withConfigurationChanger(DeprecatedValueConfiguration.KEY, false, change -> {}, changer -> {
            //noinspection CastToIncompatibleInterface
            var root = (DeprecatedValueView) changer.superRoot().getRoot(DeprecatedValueConfiguration.KEY);
            // Deprecated value should still be read as a default.
            assertEquals(10, root.intValue());

            // Check that deprecated value has been deleted while starting the changer.
            assertEquals(
                    mapWithNulls("root.intValue", null),
                    lastWriteCapture.getValue()
            );

            // Check that deprecated value is not present in the storage anymore.
            ReadEntry data = getData();
            assertEquals(2, data.changeId());
            assertEquals(
                    Map.of("root.child.my-int-cfg", 99),
                    data.values()
            );

            // Deprecated values should NOT be re-written if they're already absent.
            changeConfiguration(changer, superRoot ->
                    superRoot.changeRoot(DeprecatedValueConfiguration.KEY).changeChild().changeIntCfg(100)
            );

            assertEquals(
                    Map.of("root.child.my-int-cfg", 100),
                    lastWriteCapture.getValue()
            );

            data = getData();
            assertEquals(3, data.changeId());
            assertEquals(
                    Map.of("root.child.my-int-cfg", 100),
                    data.values()
            );
        });
    }

    @Test
    void testDeprecationReturnsDefaultValue() {
        ConfigurationMigrator changeIntValue = change -> change.changeRoot(BeforeDeprecationConfiguration.KEY).changeIntValue(999);

        withConfigurationChanger(BeforeDeprecationConfiguration.KEY, true, changeIntValue, changer -> {});

        withConfigurationChanger(DeprecatedValueConfiguration.KEY, false, change -> {}, changer -> {
            //noinspection CastToIncompatibleInterface
            var root = (DeprecatedValueView) changer.superRoot().getRoot(DeprecatedValueConfiguration.KEY);
            // Deprecated value should be read as a default.
            assertEquals(10, root.intValue());
        });
    }

    @Test
    void testConfigurationMigrator() {
        withConfigurationChanger(BeforeDeprecationConfiguration.KEY, true, change -> {}, changer -> {});

        ConfigurationMigrator migrator = superRootChange -> {
            // Check one more time that an old value can be read, just to show that we can do that inside of migrator.
            assertEquals(10, superRootChange.viewRoot(DeprecatedValueConfiguration.KEY).intValue());

            superRootChange.changeRoot(DeprecatedValueConfiguration.KEY).changeChild().changeIntCfg(100);
        };

        withConfigurationChanger(DeprecatedValueConfiguration.KEY, false, migrator, changer -> {
            //noinspection CastToIncompatibleInterface
            var root = (DeprecatedValueView) changer.superRoot().getRoot(DeprecatedValueConfiguration.KEY);
            // Migrator completed its task.
            assertEquals(100, root.child().intCfg());

            // Check that deprecated value has been deleted while starting the changer, and that migrator did its job at the same time.
            assertEquals(
                    mapWithNulls(
                            "root.intValue", null,
                            "root.child.my-int-cfg", 100
                    ),
                    lastWriteCapture.getValue()
            );
        });
    }

    @Test
    void testDeprecatedChildConfiguration() {
        withConfigurationChanger(BeforeDeprecationConfiguration.KEY, true, change -> {}, changer -> {});

        withConfigurationChanger(DeprecatedChildConfiguration.KEY, false, change -> {}, changer -> {
            //noinspection CastToIncompatibleInterface
            var root = (DeprecatedChildView) changer.superRoot().getRoot(DeprecatedChildConfiguration.KEY);
            assertNotNull(root.child());
            assertNull(root.child().strCfg()); // This test never really did anything to "strValue", that's why it's null.
            assertEquals(99, root.child().intCfg());

            // Check that deprecated value has been deleted while starting the changer.
            assertEquals(
                    mapWithNulls(
                            "root.child.my-int-cfg", null
                    ),
                    lastWriteCapture.getValue()
            );

            // Check that deprecated value is not present in the storage anymore.
            ReadEntry data = getData();

            assertEquals(2, data.changeId());
            assertEquals(
                    Map.of("root.intValue", 10),
                    data.values()
            );

            // Deprecated values should NOT be re-written if they're already absent.
            changeConfiguration(changer, superRoot ->
                    superRoot.changeRoot(DeprecatedChildConfiguration.KEY).changeIntValue(20)
            );

            assertEquals(
                    Map.of("root.intValue", 20),
                    lastWriteCapture.getValue()
            );

            data = getData();
            assertEquals(3, data.changeId());
            assertEquals(
                    Map.of("root.intValue", 20),
                    data.values()
            );
        });
    }

    @Test
    void testDeprecatedNamedListConfiguration() {
        AtomicReference<UUID> internalIdReference = new AtomicReference<>();

        withConfigurationChanger(BeforeDeprecationConfiguration.KEY, true, change -> {}, changer -> {
            // Add named list element for the test.
            changeConfiguration(changer, superRoot -> {
                superRoot.changeRoot(BeforeDeprecationConfiguration.KEY)
                        .changeList()
                        .create("foo", namedElementChange ->
                                internalIdReference.set(((InnerNode) namedElementChange).internalId())
                        );
            });

            UUID internalId = internalIdReference.get();
            assertNotNull(internalId);

            // Check that all expected properties have been added.
            assertEquals(
                    Map.of(
                            "root.list." + internalId + ".<order>", 0,
                            "root.list." + internalId + ".<name>", "foo",
                            "root.list.<ids>.foo", internalId
                    ),
                    lastWriteCapture.getValue()
            );

            // Check storage state.
            ReadEntry data = getData();

            assertEquals(2, data.changeId());
            assertEquals(
                    Map.of(
                            "root.intValue", 10,
                            "root.child.my-int-cfg", 99,
                            "root.list.<ids>.foo", internalId,
                            "root.list." + internalId + ".<order>", 0,
                            "root.list." + internalId + ".<name>", "foo"
                    ),
                    data.values()
            );
        });

        withConfigurationChanger(DeprecatedNamedListConfiguration.KEY, false, change -> {}, changer -> {
            //noinspection CastToIncompatibleInterface
            var root = (DeprecatedNamedListView) changer.superRoot().getRoot(DeprecatedNamedListConfiguration.KEY);
            assertNotNull(root.list());

            UUID internalId = internalIdReference.get();

            // Check that deprecated value has been deleted while starting the changer.
            assertEquals(
                    mapWithNulls(
                            "root.list." + internalId + ".<order>", null,
                            "root.list." + internalId + ".<name>", null,
                            "root.list.<ids>.foo", null
                    ),
                    lastWriteCapture.getValue()
            );

            // Check that deprecated value is not present in the storage anymore.
            ReadEntry data = getData();

            assertEquals(3, data.changeId());
            assertEquals(
                    Map.of(
                            "root.intValue", 10,
                            "root.child.my-int-cfg", 99
                    ),
                    data.values()
            );

            // Deprecated values should NOT be re-written if they're already absent.
            changeConfiguration(changer, superRoot ->
                    superRoot.changeRoot(DeprecatedNamedListConfiguration.KEY).changeIntValue(20)
            );

            assertEquals(
                    Map.of("root.intValue", 20),
                    lastWriteCapture.getValue()
            );

            data = getData();
            assertEquals(4, data.changeId());
            assertEquals(
                    Map.of(
                            "root.intValue", 20,
                            "root.child.my-int-cfg", 99
                    ),
                    data.values()
            );
        });
    }

    @Test
    void testDeprecatedValueInNamedListConfiguration() {
        AtomicReference<UUID> internalIdReference = new AtomicReference<>();

        withConfigurationChanger(BeforeDeprecationConfiguration.KEY, true, change -> {}, changer -> {
            // Add named list element for the test.
            changeConfiguration(changer, superRoot -> {
                superRoot.changeRoot(BeforeDeprecationConfiguration.KEY)
                        .changeList()
                        .create("foo", namedElementChange -> {
                            internalIdReference.set(((InnerNode) namedElementChange).internalId());
                            namedElementChange.changeStrCfg("value");
                        }
                        );
            });

            UUID internalId = internalIdReference.get();
            assertNotNull(internalId);

            // Check that all expected properties have been added.
            assertEquals(
                    Map.of(
                            "root.list." + internalId + ".<order>", 0,
                            "root.list." + internalId + ".<name>", "foo",
                            "root.list." + internalId + ".strCfg", "value",
                            "root.list.<ids>.foo", internalId
                    ),
                    lastWriteCapture.getValue()
            );

            // Check storage state.
            ReadEntry data = getData();

            assertEquals(2, data.changeId());
            assertEquals(
                    Map.of(
                            "root.intValue", 10,
                            "root.child.my-int-cfg", 99,
                            "root.list.<ids>.foo", internalId,
                            "root.list." + internalId + ".<order>", 0,
                            "root.list." + internalId + ".strCfg", "value",
                            "root.list." + internalId + ".<name>", "foo"
                    ),
                    data.values()
            );
        });

        withConfigurationChanger(DeprecatedFieldInNamedListConfiguration.KEY, false, change -> {}, changer -> {
            //noinspection CastToIncompatibleInterface
            var root = (DeprecatedFieldInNamedListView) changer.superRoot().getRoot(DeprecatedFieldInNamedListConfiguration.KEY);
            assertNotNull(root.list());

            UUID internalId = internalIdReference.get();

            // Check that deprecated value is accessible.
            assertEquals("value", root.list().get(internalId).strCfg());

            // Check that deprecated value has been deleted while starting the changer.
            assertEquals(
                    mapWithNulls(
                            "root.list." + internalId + ".strCfg", null
                    ),
                    lastWriteCapture.getValue()
            );

            // Check that deprecated value is not present in the storage anymore.
            ReadEntry data = getData();

            assertEquals(3, data.changeId());
            assertEquals(
                    Map.of(
                            "root.intValue", 10,
                            "root.child.my-int-cfg", 99,
                            "root.list.<ids>.foo", internalId,
                            "root.list." + internalId + ".<order>", 0,
                            "root.list." + internalId + ".<name>", "foo"
                    ),
                    data.values()
            );
        });
    }

    private void withConfigurationChanger(
            RootKey<?, ?, ?> rootKey,
            boolean init,
            ConfigurationMigrator migrator,
            Consumer<ConfigurationChanger> action
    ) {
        ConfigurationChanger changer = new TestConfigurationChanger(
                List.of(rootKey),
                storage,
                new ConfigurationTreeGenerator(rootKey),
                new TestConfigurationValidator(),
                migrator,
                s -> false
        );

        if (init) {
            changer.initializeConfigurationWith(ConfigurationUtil.EMPTY_CFG_SRC);
        }

        changer.start();

        try {
            action.accept(changer);
        } finally {
            changer.stop();
        }
    }

    private static void changeConfiguration(ConfigurationChanger changer, Consumer<SuperRootChange> changeClosure) {
        CompletableFuture<Void> changeFuture = changer.change(new ConfigurationSource() {
            @Override
            public void descend(ConstructableTreeNode node) {
                changeClosure.accept(new SuperRootChangeImpl((SuperRoot) node));
            }
        });

        assertThat(changeFuture, willCompleteSuccessfully());
    }

    private ReadEntry getData() {
        CompletableFuture<ReadEntry> dataFuture = storage.readDataOnRecovery();

        assertThat(dataFuture, willCompleteSuccessfully());

        return dataFuture.join();
    }

    /**
     * {@link Map#of} does not support null values.
     */
    private static <K, V> Map<K, V> mapWithNulls(Object... pairs) {
        assertEquals(0, pairs.length % 2);

        Map<K, V> map = new HashMap<>();

        for (int i = 0; i < pairs.length / 2; i++) {
            map.put((K) pairs[i * 2], (V) pairs[i * 2 + 1]);
        }

        return map;
    }
}
