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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.ignite.configuration.ConfigurationException;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/** */
public class ConfigurationListenerTest {
    /** */
    @ConfigurationRoot(rootName = "parent", type = LOCAL)
    public static class ParentConfigurationSchema {
        /** */
        @ConfigValue
        public ChildConfigurationSchema child;

        /** */
        @NamedConfigValue
        public ChildConfigurationSchema elements;
    }

    /** */
    @Config
    public static class ChildConfigurationSchema {
        /** */
        @Value(hasDefault = true)
        public String str = "default";

        /** Nested child configuration. */
        @ConfigValue
        public Child2ConfigurationSchema child2;

        /** Nested named child configuration. */
        @NamedConfigValue
        public Child2ConfigurationSchema elements2;
    }

    /** Scheme of the second child configuration. */
    @Config
    public static class Child2ConfigurationSchema {
        /** Integer value. */
        @Value(hasDefault = true)
        public int i = 10;
    }

    /** */
    private ConfigurationRegistry registry;

    /** */
    private ParentConfiguration configuration;

    /** */
    @BeforeEach
    public void before() {
        var testConfigurationStorage = new TestConfigurationStorage(LOCAL);

        registry = new ConfigurationRegistry(
            List.of(ParentConfiguration.KEY),
            Map.of(),
            testConfigurationStorage,
            List.of()
        );

        registry.start();

        registry.initializeDefaults();

        configuration = registry.getConfiguration(ParentConfiguration.KEY);
    }

    /** */
    @AfterEach
    public void after() {
        registry.stop();
    }

    /** */
    @Test
    public void childNode() throws Exception {
        List<String> log = new ArrayList<>();

        configuration.listen(ctx -> {
            assertEquals(ctx.oldValue().child().str(), "default");
            assertEquals(ctx.newValue().child().str(), "foo");

            log.add("parent");

            return completedFuture(null);
        });

        configuration.child().listen(ctx -> {
            assertEquals(ctx.oldValue().str(), "default");
            assertEquals(ctx.newValue().str(), "foo");

            log.add("child");

            return completedFuture(null);
        });

        configuration.child().str().listen(ctx -> {
            assertEquals(ctx.oldValue(), "default");
            assertEquals(ctx.newValue(), "foo");

            log.add("str");

            return completedFuture(null);
        });

        configuration.elements().listen(ctx -> {
            log.add("elements");

            return completedFuture(null);
        });

        configuration.change(parent -> parent.changeChild(child -> child.changeStr("foo"))).get(1, SECONDS);

        assertEquals(List.of("parent", "child", "str"), log);
    }

    /**
     * Tests notifications validity when a new named list element is created.
     */
    @Test
    public void namedListNodeOnCreate() throws Exception {
        List<String> log = new ArrayList<>();

        configuration.listen(ctx -> {
            log.add("parent");

            return completedFuture(null);
        });

        configuration.child().listen(ctx -> {
            log.add("child");

            return completedFuture(null);
        });

        configuration.elements().listen(ctx -> {
            assertEquals(0, ctx.oldValue().size());

            ChildView newValue = ctx.newValue().get("name");

            assertNotNull(newValue);
            assertEquals("default", newValue.str());

            log.add("elements");

            return completedFuture(null);
        });

        configuration.elements().listenElements(new ConfigurationNamedListListener<ChildView>() {
            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<ChildView> ctx) {
                assertNull(ctx.oldValue());

                ChildView newValue = ctx.newValue();

                assertNotNull(newValue);
                assertEquals("default", newValue.str());

                log.add("create");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("update");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onRename(
                String oldName,
                String newName,
                ConfigurationNotificationEvent<ChildView> ctx
            ) {
                log.add("rename");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("delete");

                return completedFuture(null);
            }
        });

        configuration.change(parent ->
            parent.changeElements(elements -> elements.create("name", element -> {}))
        ).get(1, SECONDS);

        assertEquals(List.of("parent", "elements", "create"), log);
    }

    /**
     * Tests notifications validity when a named list element is edited.
     */
    @Test
    public void namedListNodeOnUpdate() throws Exception {
        configuration.change(parent ->
            parent.changeElements(elements -> elements.create("name", element -> {}))
        ).get(1, SECONDS);

        List<String> log = new ArrayList<>();

        configuration.listen(ctx -> {
            log.add("parent");

            return completedFuture(null);
        });

        configuration.child().listen(ctx -> {
            log.add("child");

            return completedFuture(null);
        });

        configuration.elements().listen(ctx -> {

            ChildView oldValue = ctx.oldValue().get("name");

            assertNotNull(oldValue);
            assertEquals("default", oldValue.str());

            ChildView newValue = ctx.newValue().get("name");

            assertNotNull(newValue);
            assertEquals("foo", newValue.str());

            log.add("elements");

            return completedFuture(null);
        });

        configuration.elements().listenElements(new ConfigurationNamedListListener<ChildView>() {
            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("create");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<ChildView> ctx) {
                ChildView oldValue = ctx.oldValue();

                assertNotNull(oldValue);
                assertEquals("default", oldValue.str());

                ChildView newValue = ctx.newValue();

                assertNotNull(newValue);
                assertEquals("foo", newValue.str());

                log.add("update");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onRename(
                String oldName,
                String newName,
                ConfigurationNotificationEvent<ChildView> ctx
            ) {
                log.add("rename");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("delete");

                return completedFuture(null);
            }
        });

        configuration.change(parent ->
            parent.changeElements(elements -> elements.createOrUpdate("name", element -> element.changeStr("foo")))
        ).get(1, SECONDS);

        assertEquals(List.of("parent", "elements", "update"), log);
    }

    /**
     * Tests notifications validity when a named list element is renamed.
     */
    @Test
    public void namedListNodeOnRename() throws Exception {
        configuration.change(parent ->
            parent.changeElements(elements -> elements.create("name", element -> {}))
        ).get(1, SECONDS);

        List<String> log = new ArrayList<>();

        configuration.listen(ctx -> {
            log.add("parent");

            return completedFuture(null);
        });

        configuration.child().listen(ctx -> {
            log.add("child");

            return completedFuture(null);
        });

        configuration.elements().listen(ctx -> {
            assertEquals(1, ctx.oldValue().size());

            ChildView oldValue = ctx.oldValue().get("name");

            assertNotNull(oldValue);
            assertEquals("default", oldValue.str());

            assertEquals(1, ctx.newValue().size());

            ChildView newValue = ctx.newValue().get("newName");

            assertSame(oldValue, newValue);

            log.add("elements");

            return completedFuture(null);
        });

        configuration.elements().listenElements(new ConfigurationNamedListListener<ChildView>() {
            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("create");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("update");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onRename(
                String oldName,
                String newName,
                ConfigurationNotificationEvent<ChildView> ctx
            ) {
                assertEquals("name", oldName);
                assertEquals("newName", newName);

                ChildView oldValue = ctx.oldValue();

                assertNotNull(oldValue);
                assertEquals("default", oldValue.str());

                ChildView newValue = ctx.newValue();

                assertSame(oldValue, newValue);

                log.add("rename");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("delete");

                return completedFuture(null);
            }
        });

        configuration.change(parent ->
            parent.changeElements(elements -> elements.rename("name", "newName"))
        ).get(1, SECONDS);

        assertEquals(List.of("parent", "elements", "rename"), log);
    }

    /**
     * Tests notifications validity when a named list element is renamed and updated at the same time.
     */
    @Test
    public void namedListNodeOnRenameAndUpdate() throws Exception {
        configuration.change(parent ->
            parent.changeElements(elements -> elements.create("name", element -> {}))
        ).get(1, SECONDS);

        List<String> log = new ArrayList<>();

        configuration.listen(ctx -> {
            log.add("parent");

            return completedFuture(null);
        });

        configuration.child().listen(ctx -> {
            log.add("child");

            return completedFuture(null);
        });

        configuration.elements().listen(ctx -> {
            assertEquals(1, ctx.oldValue().size());

            ChildView oldValue = ctx.oldValue().get("name");

            assertNotNull(oldValue);
            assertEquals("default", oldValue.str());

            assertEquals(1, ctx.newValue().size());

            ChildView newValue = ctx.newValue().get("newName");

            assertNotNull(newValue, ctx.newValue().namedListKeys().toString());
            assertEquals("foo", newValue.str());

            log.add("elements");

            return completedFuture(null);
        });

        configuration.elements().listenElements(new ConfigurationNamedListListener<ChildView>() {
            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("create");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("update");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onRename(
                String oldName,
                String newName,
                ConfigurationNotificationEvent<ChildView> ctx
            ) {
                assertEquals("name", oldName);
                assertEquals("newName", newName);

                ChildView oldValue = ctx.oldValue();

                assertNotNull(oldValue);
                assertEquals("default", oldValue.str());

                ChildView newValue = ctx.newValue();

                assertNotNull(newValue);
                assertEquals("foo", newValue.str());

                log.add("rename");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("delete");

                return completedFuture(null);
            }
        });

        configuration.change(parent ->
            parent.changeElements(elements -> elements
                .rename("name", "newName")
                .createOrUpdate("newName", element -> element.changeStr("foo"))
            )
        ).get(1, SECONDS);

        assertEquals(List.of("parent", "elements", "rename"), log);
    }

    /**
     * Tests notifications validity when a named list element is deleted.
     */
    @Test
    public void namedListNodeOnDelete() throws Exception {
        configuration.change(parent ->
            parent.changeElements(elements -> elements.create("name", element -> {}))
        ).get(1, SECONDS);

        List<String> log = new ArrayList<>();

        configuration.listen(ctx -> {
            log.add("parent");

            return completedFuture(null);
        });

        configuration.child().listen(ctx -> {
            log.add("child");

            return completedFuture(null);
        });

        configuration.elements().listen(ctx -> {
            assertEquals(0, ctx.newValue().size());

            ChildView oldValue = ctx.oldValue().get("name");

            assertNotNull(oldValue);
            assertEquals("default", oldValue.str());

            log.add("elements");

            return completedFuture(null);
        });

        configuration.elements().listenElements(new ConfigurationNamedListListener<ChildView>() {
            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("create");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("update");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onRename(
                String oldName,
                String newName,
                ConfigurationNotificationEvent<ChildView> ctx
            ) {
                log.add("rename");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<ChildView> ctx) {
                assertNull(ctx.newValue());

                ChildView oldValue = ctx.oldValue();

                assertNotNull(oldValue);
                assertEquals("default", oldValue.str());

                log.add("delete");

                return completedFuture(null);
            }
        });

        configuration.elements().get("name").listen(ctx -> {
            return completedFuture(null);
        });

        configuration.change(parent ->
            parent.changeElements(elements -> elements.delete("name"))
        ).get(1, SECONDS);

        assertEquals(List.of("parent", "elements", "delete"), log);
    }

    /**
     * Tests that concurrent configuration access does not affect configuration listeners.
     */
    @Test
    public void dataRace() throws Exception {
        configuration.change(parent -> parent.changeElements(elements ->
            elements.create("name", e -> {}))
        ).get(1, SECONDS);

        CountDownLatch wait = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        List<String> log = new ArrayList<>();

        configuration.listen(ctx -> {
            try {
                wait.await(1, SECONDS);
            }
            catch (InterruptedException e) {
                fail(e.getMessage());
            }

            release.countDown();

            return completedFuture(null);
        });

        configuration.elements().get("name").listen(ctx -> {
            assertNull(ctx.newValue());

            log.add("deleted");

            return completedFuture(null);
        });

        Future<Void> fut = configuration.change(parent -> parent.changeElements(elements ->
            elements.delete("name"))
        );

        wait.countDown();

        configuration.elements();

        release.await(1, SECONDS);

        fut.get(1, SECONDS);

        assertEquals(List.of("deleted"), log);
    }

    /** */
    @Test
    void testNoGetOrUpdateConfigValueForAny() throws Exception {
        ChildConfiguration any0 = configuration.elements().any();

        assertThrows(ConfigurationException.class, () -> any0.value());
        assertThrows(ConfigurationException.class, () -> any0.change(doNothingConsumer()));

        assertThrows(ConfigurationException.class, () -> any0.str().value());
        assertThrows(ConfigurationException.class, () -> any0.str().update(""));

        assertThrows(ConfigurationException.class, () -> any0.child2().value());
        assertThrows(ConfigurationException.class, () -> any0.child2().change(doNothingConsumer()));

        assertThrows(ConfigurationException.class, () -> any0.child2().i().value());
        assertThrows(ConfigurationException.class, () -> any0.child2().i().update(100));

        assertThrows(ConfigurationException.class, () -> any0.elements2().value());
        assertThrows(ConfigurationException.class, () -> any0.elements2().change(doNothingConsumer()));
        assertThrows(ConfigurationException.class, () -> any0.elements2().get("test"));

        Child2Configuration any1 = any0.elements2().any();

        assertThrows(ConfigurationException.class, () -> any1.value());
        assertThrows(ConfigurationException.class, () -> any1.change(doNothingConsumer()));

        assertThrows(ConfigurationException.class, () -> any1.i().value());
        assertThrows(ConfigurationException.class, () -> any1.i().update(200));

        configuration.elements().change(c0 -> c0.create("test", c1 -> c1.changeStr("foo"))).get(1, SECONDS);

        Child2Configuration any2 = configuration.elements().get("test").elements2().any();

        assertThrows(ConfigurationException.class, () -> any2.value());
        assertThrows(ConfigurationException.class, () -> any2.change(doNothingConsumer()));

        assertThrows(ConfigurationException.class, () -> any2.i().value());
        assertThrows(ConfigurationException.class, () -> any2.i().update(300));
    }

    /** */
    @Test
    void testAnyListeners() throws Exception {
        List<String> events = new ArrayList<>();

        // Add "regular" listeners.
        configuration.listen(configListener(ctx -> events.add("root")));

        configuration.child().listen(configListener(ctx -> events.add("root.child")));
        configuration.child().str().listen(configListener(ctx -> events.add("root.child.str")));
        configuration.child().child2().listen(configListener(ctx -> events.add("root.child.child2")));
        configuration.child().child2().i().listen(configListener(ctx -> events.add("root.child.child2.i")));

        configuration.elements().listen(configListener(ctx -> events.add("root.elements")));
        configuration.elements().listenElements(configNamedListenerOnCreate(ctx -> events.add("root.elements.onCrt")));
        configuration.elements().listenElements(configNamedListenerOnUpdate(ctx -> events.add("root.elements.onUpd")));
        configuration.elements().listenElements(configNamedListenerOnRename(ctx -> events.add("root.elements.onRen")));
        configuration.elements().listenElements(configNamedListenerOnDelete(ctx -> events.add("root.elements.onDel")));

        configuration.elements().change(c -> c.create("0", doNothingConsumer())).get(1, SECONDS);

        ChildConfiguration childCfg = this.configuration.elements().get("0");

        childCfg.listen(configListener(ctx -> events.add("root.elements.0")));
        childCfg.str().listen(configListener(ctx -> events.add("root.elements.0.str")));
        childCfg.child2().listen(configListener(ctx -> events.add("root.elements.0.child2")));
        childCfg.child2().i().listen(configListener(ctx -> events.add("root.elements.0.child2.i")));

        NamedConfigurationTree<Child2Configuration, Child2View, Child2Change> elements2 = childCfg.elements2();

        elements2.listen(configListener(ctx -> events.add("root.elements.0.elements2")));
        elements2.listenElements(configNamedListenerOnCreate(ctx -> events.add("root.elements.0.elements2.onCrt")));
        elements2.listenElements(configNamedListenerOnUpdate(ctx -> events.add("root.elements.0.elements2.onUpd")));
        elements2.listenElements(configNamedListenerOnRename(ctx -> events.add("root.elements.0.elements2.onRen")));
        elements2.listenElements(configNamedListenerOnDelete(ctx -> events.add("root.elements.0.elements2.onDel")));

        elements2.change(c -> c.create("0", doNothingConsumer())).get(1, SECONDS);

        Child2Configuration child2 = elements2.get("0");

        child2.listen(configListener(ctx -> events.add("root.elements.0.elements2.0")));
        child2.i().listen(configListener(ctx -> events.add("root.elements.0.elements2.0.i")));

        // Adding "any" listeners.
        ChildConfiguration anyChild = configuration.elements().any();

        anyChild.listen(configListener(ctx -> events.add("root.elements.any")));
        anyChild.str().listen(configListener(ctx -> events.add("root.elements.any.str")));
        anyChild.child2().listen(configListener(ctx -> events.add("root.elements.any.child2")));
        anyChild.child2().i().listen(configListener(ctx -> events.add("root.elements.any.child2.i")));

        NamedConfigurationTree<Child2Configuration, Child2View, Child2Change> anyEl2 = anyChild.elements2();

        anyEl2.listen(configListener(ctx -> events.add("root.elements.any.elements2")));
        anyEl2.listenElements(configNamedListenerOnCreate(ctx -> events.add("root.elements.any.elements2.onCrt")));
        anyEl2.listenElements(configNamedListenerOnUpdate(ctx -> events.add("root.elements.any.elements2.onUpd")));
        anyEl2.listenElements(configNamedListenerOnRename(ctx -> events.add("root.elements.any.elements2.onRen")));
        anyEl2.listenElements(configNamedListenerOnDelete(ctx -> events.add("root.elements.any.elements2.onDel")));

        Child2Configuration anyChild2 = anyEl2.any();

        anyChild2.listen(configListener(ctx -> events.add("root.elements.any.elements2.any")));
        anyChild2.i().listen(configListener(ctx -> events.add("root.elements.any.elements2.any.i")));

        childCfg.elements2().any().listen(configListener(ctx -> events.add("root.elements.0.elements2.any")));
        childCfg.elements2().any().i().listen(configListener(ctx -> events.add("root.elements.0.elements2.any.i")));

        // Tests.
        checkListeners(
            () -> configuration.child().change(c -> c.changeStr("x").changeChild2(c0 -> c0.changeI(100))),
            List.of("root", "root.child", "root.child.str", "root.child.child2", "root.child.child2.i"),
            events
        );

        checkListeners(
            () -> configuration.elements().get("0").str().update("x"),
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
            () -> configuration.elements().get("0").elements2().get("0").i().update(200),
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

        checkListeners(
            () -> configuration.elements().get("0").elements2().change(c -> c.create("1", doNothingConsumer())),
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
                "root.elements.any.elements2.onUpd",
                "root.elements.0.elements2.onUpd"
            ),
            events
        );

        checkListeners(
            () -> configuration.elements()
                .change(c -> c.create("1", c0 -> c0.changeElements2(c1 -> c1.create("2", doNothingConsumer())))),
            List.of(
                "root",
                "root.elements",
                "root.elements.onCrt",
                "root.elements.onUpd"
            ),
            events
        );

        checkListeners(
            () -> configuration.elements().get("1").elements2().get("2").i().update(200),
            List.of(
                "root",
                "root.elements",
                "root.elements.onUpd",
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

        checkListeners(
            () -> configuration.elements().get("1").elements2().change(c -> c.rename("2", "2x")),
            List.of(
                "root",
                "root.elements",
                "root.elements.onUpd",
                "root.elements.onUpd",
                //
                "root.elements.any",
                "root.elements.any.elements2",
                "root.elements.any.elements2.onRen"
            ),
            events
        );

        checkListeners(
            () -> configuration.elements().get("1").elements2().change(c -> c.delete("2x")),
            List.of(
                "root",
                "root.elements",
                "root.elements.onUpd",
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
        return ctx -> {
            consumer.accept(ctx);

            return completedFuture(null);
        };
    }

    /**
     * @param consumer Consumer of the notification context.
     * @return Named config value change listener.
     */
    private static <T> ConfigurationNamedListListener<T> configNamedListenerOnCreate(
        Consumer<ConfigurationNotificationEvent<T>> consumer
    ) {
        return new ConfigurationNamedListListener<T>() {
            /** {@inheritDoc} */
            @Override public @NotNull CompletableFuture<?> onUpdate(@NotNull ConfigurationNotificationEvent<T> ctx) {
                return completedFuture(null);
            }

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
        return new ConfigurationNamedListListener<T>() {
            /** {@inheritDoc} */
            @Override public @NotNull CompletableFuture<?> onUpdate(@NotNull ConfigurationNotificationEvent<T> ctx) {
                return completedFuture(null);
            }

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
        return new ConfigurationNamedListListener<T>() {
            /** {@inheritDoc} */
            @Override public @NotNull CompletableFuture<?> onUpdate(@NotNull ConfigurationNotificationEvent<T> ctx) {
                return completedFuture(null);
            }

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
