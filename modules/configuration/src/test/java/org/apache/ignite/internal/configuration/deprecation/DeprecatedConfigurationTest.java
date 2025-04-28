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
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.ConfigurationChanger;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.SuperRoot;
import org.apache.ignite.internal.configuration.TestConfigurationChanger;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.Data;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNode;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.TraversableTreeNodeTest.ChildConfigurationSchema;
import org.apache.ignite.internal.configuration.tree.TraversableTreeNodeTest.NamedElementConfigurationSchema;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;

// TODO Add tests for repeated deletions. We don't need to have the same delete commands every time, makes no sense.
/**
 * Tests for configuration schemas with {@link Deprecated} properties.
 */
public class DeprecatedConfigurationTest {
    private static final ConfigurationType TEST_CONFIGURATION_TYPE = ConfigurationType.LOCAL;

    private ConfigurationStorage storage;

    @SuppressWarnings("unchecked")
    private final ArgumentCaptor<Map<String, Serializable>> lastWriteCapture = ArgumentCaptor.forClass(Map.class);

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

    @BeforeEach
    void setUp() {
        storage = spy(new TestConfigurationStorage(TEST_CONFIGURATION_TYPE));

        when(storage.write(lastWriteCapture.capture(), anyLong())).thenAnswer(InvocationOnMock::callRealMethod);
    }

    @AfterEach
    void tearDown() {
        storage.close();
    }

    /**
     * Common assertions for the initial state of the test.
     */
    @Test
    void testInitialState() {
        withConfigurationChanger(BeforeDeprecationConfiguration.KEY, true, changer -> {
            assertEquals(
                    mapWithNulls(
                            "root.intValue", 10,
                            "root.child.my-int-cfg", 99,
                            "root.child.strCfg", null
                    ),
                    lastWriteCapture.getValue()
            );

            Data data = getData();

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
        withConfigurationChanger(BeforeDeprecationConfiguration.KEY, true, changer -> {});

        withConfigurationChanger(DeprecatedValueConfiguration.KEY, false, changer -> {
            assertEquals(
                    mapWithNulls("root.intValue", null),
                    lastWriteCapture.getValue()
            );

            Data data = getData();

            assertEquals(2, data.changeId());
            assertEquals(
                    Map.of("root.child.my-int-cfg", 99),
                    data.values()
            );
        });
    }

    @Test
    void testDeprecatedChildConfiguration() {
        withConfigurationChanger(BeforeDeprecationConfiguration.KEY, true, changer -> {});

        withConfigurationChanger(DeprecatedChildConfiguration.KEY, false, changer -> {
            assertEquals(
                    mapWithNulls(
                            "root.child.my-int-cfg", null,
                            "root.child.strCfg", null
                    ),
                    lastWriteCapture.getValue()
            );

            Data data = getData();

            assertEquals(2, data.changeId());
            assertEquals(
                    Map.of("root.intValue", 10),
                    data.values()
            );
        });
    }

    @Test
    void testDeprecatedNamedListConfiguration() {
        AtomicReference<UUID> internalIdReference = new AtomicReference<>();

        withConfigurationChanger(BeforeDeprecationConfiguration.KEY, true, changer -> {
            CompletableFuture<Void> changeFuture = changer.change(new ConfigurationSource() {
                @Override
                public void descend(ConstructableTreeNode node) {
                    node.construct("root", ConfigurationUtil.EMPTY_CFG_SRC, true);

                    //noinspection CastToIncompatibleInterface
                    var beforeDeprecationChange = (BeforeDeprecationChange) ((SuperRoot) node).getRoot(BeforeDeprecationConfiguration.KEY);

                    assertNotNull(beforeDeprecationChange);

                    beforeDeprecationChange.changeList().create("foo", namedElementChange -> {
                        internalIdReference.set(((InnerNode) namedElementChange).internalId());
                    });
                }
            });

            assertThat(changeFuture, willCompleteSuccessfully());

            UUID internalId = internalIdReference.get();
            assertNotNull(internalId);

            assertEquals(
                    mapWithNulls(
                            "root.list." + internalId + ".<order>", 0,
                            "root.list." + internalId + ".<name>", "foo",
                            "root.list." + internalId + ".strCfg", null,
                            "root.list.<ids>.foo", internalId
                    ),
                    lastWriteCapture.getValue()
            );

            Data data = getData();

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

        withConfigurationChanger(DeprecatedNamedListConfiguration.KEY, false, changer -> {
            UUID internalId = internalIdReference.get();

            assertEquals(
                    mapWithNulls(
                            "root.list." + internalId + ".<order>", null,
                            "root.list." + internalId + ".<name>", null,
                            "root.list." + internalId + ".strCfg", null,
                            "root.list.<ids>.foo", null
                    ),
                    lastWriteCapture.getValue()
            );

            Data data = getData();

            assertEquals(3, data.changeId());
            assertEquals(
                    Map.of(
                            "root.intValue", 10,
                            "root.child.my-int-cfg", 99
                    ),
                    data.values()
            );
        });
    }

    private TestConfigurationChanger createChanger(RootKey<?, ?> rootKey) {
        return new TestConfigurationChanger(
                List.of(rootKey),
                storage,
                new ConfigurationTreeGenerator(rootKey),
                new TestConfigurationValidator()
        );
    }

    private void withConfigurationChanger(RootKey<?, ?> rootKey, boolean init, Consumer<ConfigurationChanger> action) {
        ConfigurationChanger changer = createChanger(rootKey);

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

    private Data getData() {
        CompletableFuture<Data> dataFuture = storage.readDataOnRecovery();

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
