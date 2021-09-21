/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.configuration.notifications;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.ignite.configuration.ConfigurationListenOnlyException;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Class for testing notification of listeners for a {@link NamedConfigurationTree#any}.
 */
public class ConfigurationAnyListenerTest {
    /**
     * Root configuration schema.
     */
    @ConfigurationRoot(rootName = "root", type = LOCAL)
    public static class RootConfigurationSchema {
        /** Nested child configuration. */
        @ConfigValue
        public FirstSubConfigurationSchema child;

        /** Nested named child configuration. */
        @NamedConfigValue
        public FirstSubConfigurationSchema elements;
    }

    /**
     * First sub-configuration schema.
     */
    @Config
    public static class FirstSubConfigurationSchema {
        /** String value. */
        @Value(hasDefault = true)
        public String str = "default";

        /** Nested child configuration. */
        @ConfigValue
        public SecondSubConfigurationSchema child2;

        /** Nested named child configuration. */
        @NamedConfigValue
        public SecondSubConfigurationSchema elements2;
    }

    /**
     * Second sub-configuration schema.
     */
    @Config
    public static class SecondSubConfigurationSchema {
        /** Integer value. */
        @Value(hasDefault = true)
        public int i = 10;
    }

    /** Configuration registry. */
    private ConfigurationRegistry registry;

    /** Root configuration. */
    private RootConfiguration rootConfig;

    /** Notification events. */
    private final List<String> events = new ArrayList<>();

    /** */
    @BeforeEach
    public void before() throws Exception {
        registry = new ConfigurationRegistry(
            List.of(RootConfiguration.KEY),
            Map.of(),
            new TestConfigurationStorage(LOCAL),
            List.of()
        );

        registry.start();

        registry.initializeDefaults();

        rootConfig = registry.getConfiguration(RootConfiguration.KEY);

        // Add "regular" listeners.
        rootConfig.listen(configListener(ctx -> events.add("root")));

        rootConfig.child().listen(configListener(ctx -> events.add("root.child")));
        rootConfig.child().str().listen(configListener(ctx -> events.add("root.child.str")));
        rootConfig.child().child2().listen(configListener(ctx -> events.add("root.child.child2")));
        rootConfig.child().child2().i().listen(configListener(ctx -> events.add("root.child.child2.i")));

        rootConfig.elements().listen(configListener(ctx -> events.add("root.elements")));
        rootConfig.elements().listenElements(configNamedListenerOnCreate(ctx -> events.add("root.elements.onCrt")));
        rootConfig.elements().listenElements(configNamedListenerOnUpdate(ctx -> events.add("root.elements.onUpd")));
        rootConfig.elements().listenElements(configNamedListenerOnRename(ctx -> events.add("root.elements.onRen")));
        rootConfig.elements().listenElements(configNamedListenerOnDelete(ctx -> events.add("root.elements.onDel")));

        rootConfig.elements().change(c -> c.create("0", doNothingConsumer())).get(1, SECONDS);

        FirstSubConfiguration childCfg = this.rootConfig.elements().get("0");

        childCfg.listen(configListener(ctx -> events.add("root.elements.0")));
        childCfg.str().listen(configListener(ctx -> events.add("root.elements.0.str")));
        childCfg.child2().listen(configListener(ctx -> events.add("root.elements.0.child2")));
        childCfg.child2().i().listen(configListener(ctx -> events.add("root.elements.0.child2.i")));

        NamedConfigurationTree<SecondSubConfiguration, SecondSubView, SecondSubChange> elements2 = childCfg.elements2();

        elements2.listen(configListener(ctx -> events.add("root.elements.0.elements2")));
        elements2.listenElements(configNamedListenerOnCreate(ctx -> events.add("root.elements.0.elements2.onCrt")));
        elements2.listenElements(configNamedListenerOnUpdate(ctx -> events.add("root.elements.0.elements2.onUpd")));
        elements2.listenElements(configNamedListenerOnRename(ctx -> events.add("root.elements.0.elements2.onRen")));
        elements2.listenElements(configNamedListenerOnDelete(ctx -> events.add("root.elements.0.elements2.onDel")));

        elements2.change(c -> c.create("0", doNothingConsumer())).get(1, SECONDS);

        SecondSubConfiguration child2 = elements2.get("0");

        child2.listen(configListener(ctx -> events.add("root.elements.0.elements2.0")));
        child2.i().listen(configListener(ctx -> events.add("root.elements.0.elements2.0.i")));

        // Adding "any" listeners.
        FirstSubConfiguration anyChild = rootConfig.elements().any();

        anyChild.listen(configListener(ctx -> events.add("root.elements.any")));
        anyChild.str().listen(configListener(ctx -> events.add("root.elements.any.str")));
        anyChild.child2().listen(configListener(ctx -> events.add("root.elements.any.child2")));
        anyChild.child2().i().listen(configListener(ctx -> events.add("root.elements.any.child2.i")));

        NamedConfigurationTree<SecondSubConfiguration, SecondSubView, SecondSubChange> anyEl2 = anyChild.elements2();

        anyEl2.listen(configListener(ctx -> events.add("root.elements.any.elements2")));
        anyEl2.listenElements(configNamedListenerOnCreate(ctx -> events.add("root.elements.any.elements2.onCrt")));
        anyEl2.listenElements(configNamedListenerOnUpdate(ctx -> events.add("root.elements.any.elements2.onUpd")));
        anyEl2.listenElements(configNamedListenerOnRename(ctx -> events.add("root.elements.any.elements2.onRen")));
        anyEl2.listenElements(configNamedListenerOnDelete(ctx -> events.add("root.elements.any.elements2.onDel")));

        SecondSubConfiguration anyChild2 = anyEl2.any();

        anyChild2.listen(configListener(ctx -> events.add("root.elements.any.elements2.any")));
        anyChild2.i().listen(configListener(ctx -> events.add("root.elements.any.elements2.any.i")));

        childCfg.elements2().any().listen(configListener(ctx -> events.add("root.elements.0.elements2.any")));
        childCfg.elements2().any().i().listen(configListener(ctx -> events.add("root.elements.0.elements2.any.i")));
    }

    /** */
    @AfterEach
    public void after() {
        registry.stop();
    }

    /** */
    @Test
    void testNoGetOrUpdateConfigValueForAny() throws Exception {
        FirstSubConfiguration any0 = rootConfig.elements().any();

        assertThrows(ConfigurationListenOnlyException.class, () -> any0.value());
        assertThrows(ConfigurationListenOnlyException.class, () -> any0.change(doNothingConsumer()));

        assertThrows(ConfigurationListenOnlyException.class, () -> any0.str().value());
        assertThrows(ConfigurationListenOnlyException.class, () -> any0.str().update(""));

        assertThrows(ConfigurationListenOnlyException.class, () -> any0.child2().value());
        assertThrows(ConfigurationListenOnlyException.class, () -> any0.child2().change(doNothingConsumer()));

        assertThrows(ConfigurationListenOnlyException.class, () -> any0.child2().i().value());
        assertThrows(ConfigurationListenOnlyException.class, () -> any0.child2().i().update(100));

        assertThrows(ConfigurationListenOnlyException.class, () -> any0.elements2().value());
        assertThrows(ConfigurationListenOnlyException.class, () -> any0.elements2().change(doNothingConsumer()));
        assertThrows(ConfigurationListenOnlyException.class, () -> any0.elements2().get("test"));

        SecondSubConfiguration any1 = any0.elements2().any();

        assertThrows(ConfigurationListenOnlyException.class, () -> any1.value());
        assertThrows(ConfigurationListenOnlyException.class, () -> any1.change(doNothingConsumer()));

        assertThrows(ConfigurationListenOnlyException.class, () -> any1.i().value());
        assertThrows(ConfigurationListenOnlyException.class, () -> any1.i().update(200));

        rootConfig.elements().change(c0 -> c0.create("test", c1 -> c1.changeStr("foo"))).get(1, SECONDS);

        SecondSubConfiguration any2 = rootConfig.elements().get("test").elements2().any();

        assertThrows(ConfigurationListenOnlyException.class, () -> any2.value());
        assertThrows(ConfigurationListenOnlyException.class, () -> any2.change(doNothingConsumer()));

        assertThrows(ConfigurationListenOnlyException.class, () -> any2.i().value());
        assertThrows(ConfigurationListenOnlyException.class, () -> any2.i().update(300));
    }

    /** */
    @Test
    void testNoAnyListenerNotification() throws Exception {
        checkListeners(
            () -> rootConfig.child().change(c -> c.changeStr("x").changeChild2(c0 -> c0.changeI(100))),
            List.of(
                "root",
                "root.child",
                "root.child.str",
                "root.child.child2",
                "root.child.child2.i"
            ),
            events
        );
    }

    /** */
    @Test
    void testAnyListenerNotificationOnCreate() throws Exception {
        checkListeners(
            () -> rootConfig.elements()
                .change(c -> c.create("1", c0 -> c0.changeElements2(c1 -> c1.create("2", doNothingConsumer())))),
            List.of(
                "root",
                "root.elements",
                "root.elements.onCrt",
                //
                "root.elements.any",
                "root.elements.any.str",
                //
                "root.elements.any.child2",
                "root.elements.any.child2.i",
                //
                "root.elements.any.elements2",
                "root.elements.any.elements2.onCrt",
                "root.elements.any.elements2.any",
                "root.elements.any.elements2.any.i"
            ),
            events
        );

        checkListeners(
            () -> rootConfig.elements().get("0").elements2().change(c -> c.create("1", doNothingConsumer())),
            List.of(
                "root",
                "root.elements",
                "root.elements.onUpd",
                //
                "root.elements.any",
                "root.elements.0",
                //
                "root.elements.any.elements2",
                "root.elements.0.elements2",
                "root.elements.any.elements2.onCrt",
                "root.elements.0.elements2.onCrt",
                //
                "root.elements.any.elements2.any",
                "root.elements.0.elements2.any",
                "root.elements.any.elements2.any.i",
                "root.elements.0.elements2.any.i"
            ),
            events
        );

        checkListeners(
            () -> rootConfig.elements().get("1").elements2().change(c -> c.create("3", doNothingConsumer())),
            List.of(
                "root",
                "root.elements",
                "root.elements.onUpd",
                //
                "root.elements.any",
                "root.elements.any.elements2",
                "root.elements.any.elements2.onCrt",
                //
                "root.elements.any.elements2.any",
                "root.elements.any.elements2.any.i"
            ),
            events
        );
    }

    /** */
    @Test
    void testAnyListenerNotificationOnRename() throws Exception {
        checkListeners(
            () -> rootConfig.elements().get("0").elements2().change(c -> c.rename("0", "0x")),
            List.of(
                "root",
                "root.elements",
                "root.elements.onUpd",
                //
                "root.elements.any",
                "root.elements.0",
                //
                "root.elements.any.elements2",
                "root.elements.0.elements2",
                //
                "root.elements.any.elements2.onRen",
                "root.elements.0.elements2.onRen"
            ),
            events
        );

        rootConfig.elements()
            .change(c -> c.create("1", c0 -> c0.changeElements2(c1 -> c1.create("2", doNothingConsumer()))))
            .get(1, SECONDS);

        checkListeners(
            () -> rootConfig.elements().get("1").elements2().change(c -> c.rename("2", "2x")),
            List.of(
                "root",
                "root.elements",
                "root.elements.onUpd",
                //
                "root.elements.any",
                "root.elements.any.elements2",
                "root.elements.any.elements2.onRen"
            ),
            events
        );
    }

    /** */
    @Test
    void testAnyListenerNotificationOnDelete() throws Exception {
        checkListeners(
            () -> rootConfig.elements().get("0").elements2().change(c -> c.delete("0")),
            List.of(
                "root",
                "root.elements",
                "root.elements.onUpd",
                //
                "root.elements.any",
                "root.elements.0",
                //
                "root.elements.any.elements2",
                "root.elements.0.elements2",
                //
                "root.elements.any.elements2.onDel",
                "root.elements.0.elements2.onDel",
                //
                "root.elements.any.elements2.any",
                "root.elements.0.elements2.0"
            ),
            events
        );

        rootConfig.elements()
            .change(c -> c.create("1", c0 -> c0.changeElements2(c1 -> c1.create("2", doNothingConsumer()))))
            .get(1, SECONDS);

        checkListeners(
            () -> rootConfig.elements().get("1").elements2().change(c -> c.delete("2")),
            List.of(
                "root",
                "root.elements",
                "root.elements.onUpd",
                //
                "root.elements.any",
                "root.elements.any.elements2",
                "root.elements.any.elements2.onDel",
                "root.elements.any.elements2.any"
            ),
            events
        );
    }

    /** */
    @Test
    void testAnyListenerNotificationForLeaf() throws Exception {
        checkListeners(
            () -> rootConfig.elements().get("0").str().update("x"),
            List.of(
                "root",
                "root.elements",
                "root.elements.onUpd",
                //
                "root.elements.any",
                "root.elements.0",
                //
                "root.elements.any.str",
                "root.elements.0.str"
            ),
            events
        );

        checkListeners(
            () -> rootConfig.elements().get("0").elements2().get("0").i().update(200),
            List.of(
                "root",
                "root.elements",
                "root.elements.onUpd",
                //
                "root.elements.any",
                "root.elements.0",
                //
                "root.elements.any.elements2",
                "root.elements.0.elements2",
                "root.elements.any.elements2.onUpd",
                "root.elements.0.elements2.onUpd",
                //
                "root.elements.any.elements2.any",
                "root.elements.0.elements2.any",
                "root.elements.0.elements2.0",
                //
                "root.elements.any.elements2.any.i",
                "root.elements.0.elements2.any.i",
                "root.elements.0.elements2.0.i"
            ),
            events
        );

        rootConfig.elements()
            .change(c -> c.create("1", c0 -> c0.changeElements2(c1 -> c1.create("2", doNothingConsumer()))))
            .get(1, SECONDS);

        checkListeners(
            () -> rootConfig.elements().get("1").elements2().get("2").i().update(200),
            List.of(
                "root",
                "root.elements",
                "root.elements.onUpd",
                //
                "root.elements.any",
                "root.elements.any.elements2",
                "root.elements.any.elements2.onUpd",
                "root.elements.any.elements2.any",
                "root.elements.any.elements2.any.i"
            ),
            events
        );
    }

    /** */
    @Test
    void testAnyStopListen() throws Exception {
        ConfigurationListener<FirstSubView> listener = configListener(ctx -> events.add("root.elements.any2"));

        rootConfig.elements().any().listen(listener);

        checkListeners(
            () -> rootConfig.elements().get("0").str().update(UUID.randomUUID().toString()),
            events,
            List.of("root.elements.any", "root.elements.any2"),
            List.of()
        );

        rootConfig.elements().any().stopListen(listener);

        checkListeners(
            () -> rootConfig.elements().get("0").str().update(UUID.randomUUID().toString()),
            events,
            List.of("root.elements.any"),
            List.of("root.elements.any2")
        );
    }

    /**
     * Helper method for testing listeners.
     *
     * @param changeFun Configuration change function.
     * @param exp Expected list of executing listeners.
     * @param act Reference to the list of executing listeners that is filled after the {@code changeFun} is executed.
     * @throws Exception If failed.
     */
    private static void checkListeners(
        Supplier<CompletableFuture<Void>> changeFun,
        List<String> exp,
        List<String> act
    ) throws Exception {
        act.clear();

        changeFun.get().get(1, SECONDS);

        assertEquals(exp, act);
    }

    /**
     * Helper method for testing listeners.
     *
     * @param changeFun Configuration change function.
     * @param events Reference to the list of executing listeners that is filled after the {@code changeFun} is executed.
     * @param expContains Listeners that are expected are contained in the {@code events}.
     * @param expNotContains Listeners that are expected are not contained in the {@code events}.
     * @throws Exception If failed.
     */
    private static void checkListeners(
        Supplier<CompletableFuture<Void>> changeFun,
        List<String> events,
        List<String> expContains,
        List<String> expNotContains
    ) throws Exception {
        events.clear();

        changeFun.get().get(1, SECONDS);

        for (String exp : expContains)
            assertTrue(events.contains(exp), () -> exp + " not contains in " + events);

        for (String exp : expNotContains)
            assertFalse(events.contains(exp), () -> exp + " contains in " + events);
    }

    /**
     * @param consumer Consumer of the notification context.
     * @return Config value change listener.
     */
    private static <T> ConfigurationListener<T> configListener(Consumer<ConfigurationNotificationEvent<T>> consumer) {
        return ctx -> {
            consumer.accept(ctx);

            return completedFuture(null);
        };
    }

    /**
     * @param consumer Consumer of the notification context.
     * @return Named config value change listener.
     */
    private static <T> ConfigurationNamedListListener<T> configNamedListenerOnUpdate(
        Consumer<ConfigurationNotificationEvent<T>> consumer
    ) {
        return new ConfigurationNamedListListener<>() {
            /** {@inheritDoc} */
            @Override public @NotNull CompletableFuture<?> onUpdate(@NotNull ConfigurationNotificationEvent<T> ctx) {
                consumer.accept(ctx);

                return completedFuture(null);
            }
        };
    }

    /**
     * @param consumer Consumer of the notification context.
     * @return Named config value change listener.
     */
    private static <T> ConfigurationNamedListListener<T> configNamedListenerOnCreate(
        Consumer<ConfigurationNotificationEvent<T>> consumer
    ) {
        return new ConfigurationNamedListListener<>() {
            /** {@inheritDoc} */
            @Override public @NotNull CompletableFuture<?> onCreate(@NotNull ConfigurationNotificationEvent<T> ctx) {
                consumer.accept(ctx);

                return completedFuture(null);
            }
        };
    }

    /**
     * @param consumer Consumer of the notification context.
     * @return Named config value change listener.
     */
    private static <T> ConfigurationNamedListListener<T> configNamedListenerOnRename(
        Consumer<ConfigurationNotificationEvent<T>> consumer
    ) {
        return new ConfigurationNamedListListener<>() {
            /** {@inheritDoc} */
            @Override public @NotNull CompletableFuture<?> onRename(
                @NotNull String oldName,
                @NotNull String newName,
                @NotNull ConfigurationNotificationEvent<T> ctx
            ) {
                consumer.accept(ctx);

                return completedFuture(null);
            }
        };
    }

    /**
     * @param consumer Consumer of the notification context.
     * @return Named config value change listener.
     */
    private static <T> ConfigurationNamedListListener<T> configNamedListenerOnDelete(
        Consumer<ConfigurationNotificationEvent<T>> consumer
    ) {
        return new ConfigurationNamedListListener<>() {
            /** {@inheritDoc} */
            @Override public @NotNull CompletableFuture<?> onDelete(@NotNull ConfigurationNotificationEvent<T> ctx) {
                consumer.accept(ctx);

                return completedFuture(null);
            }
        };
    }

    /**
     * @return Consumer who does nothing.
     */
    private static <T> Consumer<T> doNothingConsumer() {
        return t -> {
        };
    }
}
