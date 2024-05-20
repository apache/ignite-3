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

package org.apache.ignite.internal.configuration.notifications;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationListenerTestUtils.checkContainsListeners;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationListenerTestUtils.configListener;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationListenerTestUtils.configNamedListenerOnCreate;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationListenerTestUtils.configNamedListenerOnDelete;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationListenerTestUtils.configNamedListenerOnRename;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationListenerTestUtils.configNamedListenerOnUpdate;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationListenerTestUtils.doNothingConsumer;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationListenerTestUtils.randomUuid;
import static org.apache.ignite.internal.configuration.notifications.ConfigurationNotifier.notifyListeners;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.hasCause;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationExtension;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.DynamicConfiguration;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Test class for configuration listener.
 */
public class ConfigurationListenerTest {
    /**
     * Parent root configuration schema.
     */
    @ConfigurationRoot(rootName = "parent", type = LOCAL)
    public static class ParentConfigurationSchema {
        @ConfigValue
        public ChildConfigurationSchema child;

        @NamedConfigValue
        public ChildConfigurationSchema children;

        @ConfigValue
        public PolyConfigurationSchema polyChild;

        @NamedConfigValue
        public PolyConfigurationSchema polyChildren;
    }

    /**
     * Child configuration schema.
     */
    @Config
    public static class ChildConfigurationSchema {
        @Value(hasDefault = true)
        public String str = "default";

        @NamedConfigValue
        public EntryConfigurationSchema entries;
    }

    /**
     * Entry configuration schema.
     */
    @Config
    public static class EntryConfigurationSchema {
        @Value(hasDefault = true)
        public String str = "default";
    }

    /**
     * Internal extension of {@link ChildConfigurationSchema}.
     */
    @ConfigurationExtension(internal = true)
    public static class InternalChildConfigurationSchema extends ChildConfigurationSchema {
        @Value(hasDefault = true)
        public int intVal = 0;
    }

    /**
     * Base class for polymorhphic configs.
     */
    @PolymorphicConfig
    public static class PolyConfigurationSchema {
        public static final String STRING = "string";
        public static final String LONG = "long";

        @PolymorphicId(hasDefault = true)
        public String type = STRING;

        @Value(hasDefault = true)
        public int commonIntVal = 11;
    }

    /**
     * String-based variant of a polymorphic config.
     */
    @PolymorphicConfigInstance(PolyConfigurationSchema.STRING)
    public static class StringPolyConfigurationSchema extends PolyConfigurationSchema {
        @Value(hasDefault = true)
        public String specificVal = "original";
    }

    /**
     * Long-based variant of a polymorphic config.
     */
    @PolymorphicConfigInstance(PolyConfigurationSchema.LONG)
    public static class LongPolyConfigurationSchema extends PolyConfigurationSchema {
        @Value(hasDefault = true)
        public long specificVal = 12;
    }

    private ConfigurationStorage storage;

    private ConfigurationTreeGenerator generator;

    private ConfigurationRegistry registry;

    private ParentConfiguration config;

    /**
     * Before each.
     */
    @BeforeEach
    public void before() {
        storage = new TestConfigurationStorage(LOCAL);

        generator = new ConfigurationTreeGenerator(
                List.of(ParentConfiguration.KEY),
                List.of(InternalChildConfigurationSchema.class),
                List.of(StringPolyConfigurationSchema.class, LongPolyConfigurationSchema.class)
        );
        registry = new ConfigurationRegistry(
                List.of(ParentConfiguration.KEY),
                storage,
                generator,
                new TestConfigurationValidator()
        );

        assertThat(registry.startAsync(ForkJoinPool.commonPool()), willCompleteSuccessfully());
        assertThat(registry.onDefaultsPersisted(), willCompleteSuccessfully());

        config = registry.getConfiguration(ParentConfiguration.KEY);
    }

    @AfterEach
    public void after() {
        assertThat(registry.stopAsync(ForkJoinPool.commonPool()), willCompleteSuccessfully());
        generator.close();
    }

    @Test
    public void childNode() throws Exception {
        List<String> log = new ArrayList<>();

        config.listen(ctx -> {
            assertEquals(ctx.oldValue().child().str(), "default");
            assertEquals(ctx.newValue().child().str(), "foo");

            log.add("parent");

            return nullCompletedFuture();
        });

        config.child().listen(ctx -> {
            assertEquals(ctx.oldValue().str(), "default");
            assertEquals(ctx.newValue().str(), "foo");

            log.add("child");

            return nullCompletedFuture();
        });

        config.child().str().listen(ctx -> {
            assertEquals(ctx.oldValue(), "default");
            assertEquals(ctx.newValue(), "foo");

            log.add("str");

            return nullCompletedFuture();
        });

        config.children().listen(ctx -> {
            log.add("elements");

            return nullCompletedFuture();
        });

        config.change(parent -> parent.changeChild(child -> child.changeStr("foo"))).get(1, SECONDS);

        assertEquals(List.of("parent", "child", "str"), log);
    }

    /**
     * Tests notifications validity when a new named list element is created.
     */
    @Test
    public void namedListNodeOnCreate() throws Exception {
        List<String> log = new ArrayList<>();

        config.listen(ctx -> {
            log.add("parent");

            return nullCompletedFuture();
        });

        config.child().listen(ctx -> {
            log.add("child");

            return nullCompletedFuture();
        });

        config.children().listen(ctx -> {
            assertEquals(0, ctx.oldValue().size());

            ChildView newValue = ctx.newValue().get("name");

            assertNotNull(newValue);
            assertEquals("default", newValue.str());

            log.add("elements");

            return nullCompletedFuture();
        });

        config.children().listenElements(new ConfigurationNamedListListener<ChildView>() {
            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<ChildView> ctx) {
                assertNull(ctx.oldValue());

                ChildView newValue = ctx.newValue();

                assertNotNull(newValue);
                assertEquals("default", newValue.str());

                log.add("create");

                return nullCompletedFuture();
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("update");

                return nullCompletedFuture();
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onRename(
                    ConfigurationNotificationEvent<ChildView> ctx
            ) {
                log.add("rename");

                return nullCompletedFuture();
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("delete");

                return nullCompletedFuture();
            }
        });

        config.change(parent ->
                parent.changeChildren(elements -> elements.create("name", element -> {
                }))
        ).get(1, SECONDS);

        assertEquals(List.of("parent", "elements", "create"), log);
    }

    /**
     * Tests notifications validity when a named list element is edited.
     */
    @Test
    public void namedListNodeOnUpdate() throws Exception {
        config.change(parent ->
                parent.changeChildren(elements -> elements.create("name", element -> {
                }))
        ).get(1, SECONDS);

        List<String> log = new ArrayList<>();

        config.listen(ctx -> {
            log.add("parent");

            return nullCompletedFuture();
        });

        config.child().listen(ctx -> {
            log.add("child");

            return nullCompletedFuture();
        });

        config.children().listen(ctx -> {

            ChildView oldValue = ctx.oldValue().get("name");

            assertNotNull(oldValue);
            assertEquals("default", oldValue.str());

            ChildView newValue = ctx.newValue().get("name");

            assertNotNull(newValue);
            assertEquals("foo", newValue.str());

            log.add("elements");

            return nullCompletedFuture();
        });

        config.children().listenElements(new ConfigurationNamedListListener<ChildView>() {
            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("create");

                return nullCompletedFuture();
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<ChildView> ctx) {
                ChildView oldValue = ctx.oldValue();

                assertNotNull(oldValue);
                assertEquals("default", oldValue.str());

                ChildView newValue = ctx.newValue();

                assertNotNull(newValue);
                assertEquals("foo", newValue.str());

                log.add("update");

                return nullCompletedFuture();
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onRename(
                    ConfigurationNotificationEvent<ChildView> ctx
            ) {
                log.add("rename");

                return nullCompletedFuture();
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("delete");

                return nullCompletedFuture();
            }
        });

        config.change(parent ->
                parent.changeChildren(elements -> elements.createOrUpdate("name", element -> element.changeStr("foo")))
        ).get(1, SECONDS);

        assertEquals(List.of("parent", "elements", "update"), log);
    }

    /**
     * Tests notifications validity when a named list element is renamed.
     */
    @Test
    public void namedListNodeOnRename() throws Exception {
        config.change(parent ->
                parent.changeChildren(elements -> elements.create("name", element -> {
                }))
        ).get(1, SECONDS);

        List<String> log = new ArrayList<>();

        config.listen(ctx -> {
            log.add("parent");

            return nullCompletedFuture();
        });

        config.child().listen(ctx -> {
            log.add("child");

            return nullCompletedFuture();
        });

        config.children().listen(ctx -> {
            assertEquals(1, ctx.oldValue().size());

            ChildView oldValue = ctx.oldValue().get("name");

            assertNotNull(oldValue);
            assertEquals("default", oldValue.str());

            assertEquals(1, ctx.newValue().size());

            ChildView newValue = ctx.newValue().get("newName");

            assertNotSame(oldValue, newValue);

            log.add("elements");

            return nullCompletedFuture();
        });

        config.children().listenElements(new ConfigurationNamedListListener<ChildView>() {
            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("create");

                return nullCompletedFuture();
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("update");

                return nullCompletedFuture();
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onRename(
                    ConfigurationNotificationEvent<ChildView> ctx
            ) {
                assertEquals("name", ctx.oldName(ChildView.class));
                assertEquals("newName", ctx.newName(ChildView.class));

                ChildView oldValue = ctx.oldValue();

                assertNotNull(oldValue);
                assertEquals("default", oldValue.str());

                ChildView newValue = ctx.newValue();

                assertNotSame(oldValue, newValue);

                log.add("rename");

                return nullCompletedFuture();
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("delete");

                return nullCompletedFuture();
            }
        });

        config.change(parent ->
                parent.changeChildren(elements -> elements.rename("name", "newName"))
        ).get(1, SECONDS);

        assertEquals(List.of("parent", "elements", "rename"), log);
    }

    /**
     * Tests notifications validity when a named list element is renamed and updated at the same time.
     */
    @Test
    public void namedListNodeOnRenameAndUpdate() throws Exception {
        config.change(parent ->
                parent.changeChildren(elements -> elements.create("name", element -> {
                }))
        ).get(1, SECONDS);

        List<String> log = new ArrayList<>();

        config.listen(ctx -> {
            log.add("parent");

            return nullCompletedFuture();
        });

        config.child().listen(ctx -> {
            log.add("child");

            return nullCompletedFuture();
        });

        config.children().listen(ctx -> {
            assertEquals(1, ctx.oldValue().size());

            ChildView oldValue = ctx.oldValue().get("name");

            assertNotNull(oldValue);
            assertEquals("default", oldValue.str());

            assertEquals(1, ctx.newValue().size());

            ChildView newValue = ctx.newValue().get("newName");

            assertNotNull(newValue, ctx.newValue().namedListKeys().toString());
            assertEquals("foo", newValue.str());

            log.add("elements");

            return nullCompletedFuture();
        });

        config.children().listenElements(new ConfigurationNamedListListener<ChildView>() {
            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("create");

                return nullCompletedFuture();
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("update");

                return nullCompletedFuture();
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onRename(
                    ConfigurationNotificationEvent<ChildView> ctx
            ) {
                assertEquals("name", ctx.oldName(ChildView.class));
                assertEquals("newName", ctx.newName(ChildView.class));

                ChildView oldValue = ctx.oldValue();

                assertNotNull(oldValue);
                assertEquals("default", oldValue.str());

                ChildView newValue = ctx.newValue();

                assertNotNull(newValue);
                assertEquals("foo", newValue.str());

                log.add("rename");

                return nullCompletedFuture();
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("delete");

                return nullCompletedFuture();
            }
        });

        config.change(parent ->
                parent.changeChildren(elements -> elements
                        .rename("name", "newName")
                        .createOrUpdate("newName", element -> element.changeStr("foo"))
                )
        ).get(1, SECONDS);

        assertEquals(List.of("parent", "elements", "rename"), log);
    }

    /**
     * Tests notifications validity when a named list element is renamed and then updated a sub-element of the renamed element.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-21101")
    public void namedListNodeOnRenameAndThenUpdateSubElement() throws Exception {
        config.change(parent ->
                parent.changeChildren(elements -> elements.create("name", element -> {
                    element.changeEntries()
                            .create("entry", entry -> entry.changeStr("default"));
                }))
        ).get(1, SECONDS);

        List<String> log = new ArrayList<>();

        config.listen(ctx -> {
            log.add("parent");

            return nullCompletedFuture();
        });

        config.child().listen(ctx -> {
            log.add("child");

            return nullCompletedFuture();
        });

        config.children().listen(ctx -> {
            log.add("children");

            return nullCompletedFuture();
        });

        config.children().listenElements(new ConfigurationNamedListListener<ChildView>() {
            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("create");

                return nullCompletedFuture();
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("update");

                return nullCompletedFuture();
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onRename(
                    ConfigurationNotificationEvent<ChildView> ctx
            ) {
                log.add("rename");

                return nullCompletedFuture();
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("delete");

                return nullCompletedFuture();
            }
        });

        config.children().get("name").entries().get("entry").listen(ctx -> {
            log.add("entry");

            return nullCompletedFuture();
        });

        config.change(parent ->
                parent.changeChildren(elements -> elements
                        .rename("name", "newName")
                )
        ).get(1, SECONDS);

        config.children().get("newName")
                .entries()
                .get("entry")
                .str()
                .update("foo")
                .get(1, SECONDS);

        assertEquals(List.of("parent", "elements", "rename"), log);
    }


    /**
     * Tests notifications validity when a named list element is deleted.
     */
    @Test
    public void namedListNodeOnDelete() throws Exception {
        config.change(parent ->
                parent.changeChildren(elements -> elements.create("name", element -> {
                }))
        ).get(1, SECONDS);

        List<String> log = new ArrayList<>();

        config.listen(ctx -> {
            log.add("parent");

            return nullCompletedFuture();
        });

        config.child().listen(ctx -> {
            log.add("child");

            return nullCompletedFuture();
        });

        config.children().listen(ctx -> {
            assertEquals(0, ctx.newValue().size());

            ChildView oldValue = ctx.oldValue().get("name");

            assertNotNull(oldValue);
            assertEquals("default", oldValue.str());

            log.add("elements");

            return nullCompletedFuture();
        });

        config.children().listenElements(new ConfigurationNamedListListener<ChildView>() {
            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("create");

                return nullCompletedFuture();
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<ChildView> ctx) {
                log.add("update");

                return nullCompletedFuture();
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onRename(
                    ConfigurationNotificationEvent<ChildView> ctx
            ) {
                log.add("rename");

                return nullCompletedFuture();
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<ChildView> ctx) {
                assertNull(ctx.newValue());

                ChildView oldValue = ctx.oldValue();

                assertNotNull(oldValue);
                assertEquals("default", oldValue.str());

                log.add("delete");

                return nullCompletedFuture();
            }
        });

        config.children().get("name").listen(ctx -> {
            return nullCompletedFuture();
        });

        config.change(parent ->
                parent.changeChildren(elements -> elements.delete("name"))
        ).get(1, SECONDS);

        assertEquals(List.of("parent", "elements", "delete"), log);
    }

    /**
     * Tests that concurrent configuration access does not affect configuration listeners.
     */
    @Test
    public void dataRace() throws Exception {
        config.change(parent -> parent.changeChildren(elements ->
                elements.create("name", e -> {
                }))
        ).get(1, SECONDS);

        CountDownLatch wait = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        List<String> log = new ArrayList<>();

        config.listen(ctx -> {
            try {
                wait.await(1, SECONDS);
            } catch (InterruptedException e) {
                fail(e.getMessage());
            }

            release.countDown();

            return nullCompletedFuture();
        });

        config.children().get("name").listen(ctx -> {
            assertNull(ctx.newValue());

            log.add("deleted");

            return nullCompletedFuture();
        });

        Future<Void> fut = config.change(parent -> parent.changeChildren(elements ->
                elements.delete("name"))
        );

        wait.countDown();

        config.children();

        release.await(1, SECONDS);

        fut.get(1, SECONDS);

        assertEquals(List.of("deleted"), log);
    }

    @Test
    void testStopListen() throws Exception {
        List<String> events = new ArrayList<>();

        ConfigurationListener<ParentView> listener0 = configListener(ctx -> events.add("0"));
        ConfigurationListener<ParentView> listener1 = configListener(ctx -> events.add("1"));

        ConfigurationNamedListListener<ChildView> listener2 = configNamedListenerOnUpdate(ctx -> events.add("2"));
        ConfigurationNamedListListener<ChildView> listener3 = configNamedListenerOnUpdate(ctx -> events.add("3"));

        config.listen(listener0);
        config.listen(listener1);

        config.children().listenElements(listener2);
        config.children().listenElements(listener3);

        config.children().change(c -> c.create("0", doNothingConsumer())).get(1, SECONDS);

        checkContainsListeners(
                () -> config.children().get("0").str().update(randomUuid()),
                events,
                List.of("0", "1", "2", "3"),
                List.of()
        );

        config.stopListen(listener0);
        config.children().stopListenElements(listener2);

        checkContainsListeners(
                () -> config.children().get("0").str().update(randomUuid()),
                events,
                List.of("1", "3"),
                List.of("0", "2")
        );

        config.stopListen(listener1);
        config.children().stopListenElements(listener3);

        checkContainsListeners(
                () -> config.children().get("0").str().update(randomUuid()),
                events,
                List.of(),
                List.of("0", "1", "2", "3")
        );
    }

    @Test
    void testGetConfigFromNotificationEvent() throws Exception {
        String newVal = randomUuid();

        config.listen(configListener(ctx -> {
            ParentView parent = ctx.newValue(ParentView.class);

            assertNotNull(parent);
            assertNull(ctx.newName(ParentView.class));

            assertEquals(newVal, parent.child().str());
        }));

        config.child().listen(configListener(ctx -> {
            assertNotNull(ctx.newValue(ParentView.class));

            ChildView child = ctx.newValue(ChildView.class);

            assertNotNull(child);
            assertNull(ctx.newName(ChildView.class));

            assertEquals(newVal, child.str());
        }));

        config.child().str().listen(configListener(ctx -> {
            assertNotNull(ctx.newValue(ParentView.class));

            ChildView child = ctx.newValue(ChildView.class);

            assertNotNull(child);
            assertNull(ctx.newName(ChildView.class));

            assertEquals(newVal, child.str());
        }));

        config.change(c0 -> c0.changeChild(c1 -> c1.changeStr(newVal))).get(1, SECONDS);
    }

    @Test
    void testGetConfigFromNotificationEventOnCreate() throws Exception {
        String newVal = randomUuid();
        String key = randomUuid();

        config.children().listen(configListener(ctx -> {
            ParentView parent = ctx.newValue(ParentView.class);

            assertNotNull(parent);
            assertNull(ctx.newName(ParentView.class));

            assertNull(ctx.newValue(ChildView.class));
            assertNull(ctx.newName(ChildView.class));

            assertEquals(newVal, parent.children().get(key).str());
        }));

        config.children().listenElements(configNamedListenerOnCreate(ctx -> {
            assertNotNull(ctx.newValue(ParentView.class));
            assertNull(ctx.newName(ParentView.class));

            ChildView child = ctx.newValue(ChildView.class);

            assertNotNull(child);
            assertEquals(key, ctx.newName(ChildView.class));

            assertEquals(newVal, child.str());
        }));

        config.children().change(c -> c.create(key, c1 -> c1.changeStr(newVal))).get(1, SECONDS);
    }

    @Test
    void testGetConfigFromNotificationEventOnRename() throws Exception {
        String val = "default";
        String oldKey = randomUuid();
        String newKey = randomUuid();

        config.children().change(c -> c.create(oldKey, doNothingConsumer())).get(1, SECONDS);

        config.children().listen(configListener(ctx -> {
            ParentView parent = ctx.newValue(ParentView.class);

            assertNotNull(parent);
            assertNull(ctx.newName(ParentView.class));

            assertNull(ctx.newValue(ChildView.class));
            assertNull(ctx.newName(ChildView.class));

            assertNull(parent.children().get(oldKey));
            assertEquals(val, parent.children().get(newKey).str());
        }));

        config.children().listenElements(configNamedListenerOnRename(ctx -> {
            assertNotNull(ctx.newValue(ParentView.class));
            assertNull(ctx.newName(ParentView.class));

            ChildView child = ctx.newValue(ChildView.class);

            assertNotNull(child);
            assertEquals(newKey, ctx.newName(ChildView.class));

            assertEquals(val, child.str());
        }));

        config.children().change(c -> c.rename(oldKey, newKey));
    }

    @Test
    void testGetConfigFromNotificationEventOnDelete() throws Exception {
        String key = randomUuid();

        config.children().change(c -> c.create(key, doNothingConsumer())).get(1, SECONDS);

        config.children().listen(configListener(ctx -> {
            ParentView parent = ctx.newValue(ParentView.class);

            assertNotNull(parent);
            assertNull(ctx.newName(ParentView.class));

            assertNull(ctx.oldValue(ChildView.class));
            assertNull(ctx.oldName(ChildView.class));

            assertNull(ctx.newValue(ChildView.class));
            assertNull(ctx.newName(ChildView.class));

            assertNull(parent.children().get(key));
        }));

        Consumer<ConfigurationNotificationEvent<ChildView>> assertions = ctx -> {
            assertNotNull(ctx.newValue(ParentView.class));
            assertNull(ctx.newName(ParentView.class));

            assertNotNull(ctx.oldValue(ChildView.class));
            assertEquals(key, ctx.oldName(ChildView.class));

            assertNull(ctx.newValue(ChildView.class));
            assertNull(ctx.newName(ChildView.class));
        };

        config.children().listenElements(configNamedListenerOnDelete(assertions));
        config.children().get(key).listen(configListener(assertions));

        config.children().change(c -> c.delete(key)).get(1, SECONDS);
    }

    @Test
    void testGetConfigFromNotificationEventOnUpdate() throws Exception {
        String newVal = randomUuid();
        String key = randomUuid();

        config.children().change(c -> c.create(key, doNothingConsumer())).get(1, SECONDS);

        config.children().listen(configListener(ctx -> {
            ParentView parent = ctx.newValue(ParentView.class);

            assertNotNull(parent);
            assertNull(ctx.newName(ParentView.class));

            assertNull(ctx.oldValue(ChildView.class));
            assertNull(ctx.oldName(ChildView.class));

            assertNull(ctx.newValue(ChildView.class));
            assertNull(ctx.newName(ChildView.class));

            assertEquals(newVal, parent.children().get(key).str());
        }));

        Consumer<ConfigurationNotificationEvent<ChildView>> assertions = ctx -> {
            assertNotNull(ctx.newValue(ParentView.class));
            assertNull(ctx.newName(ParentView.class));

            ChildView child = ctx.newValue(ChildView.class);

            assertNotNull(child);
            assertEquals(key, ctx.newName(ChildView.class));

            assertEquals(newVal, child.str());
        };

        config.children().listenElements(configNamedListenerOnUpdate(assertions));
        config.children().get(key).listen(configListener(assertions));

        config.children().get(key).str().update(newVal).get(1, SECONDS);
    }

    @Test
    void polymorphicParentFieldChangeNotificationHappens() throws Exception {
        AtomicInteger intHolder = new AtomicInteger();

        config.polyChild().commonIntVal().listen(event -> {
            intHolder.set(event.newValue());
            return nullCompletedFuture();
        });

        config.polyChild().commonIntVal().update(42).get(1, SECONDS);

        assertThat(intHolder.get(), is(42));
    }

    @Test
    void testNotificationEventConfigForNestedConfiguration() throws Exception {
        config.child().listen(ctx -> {
            assertInstanceOf(ChildView.class, ctx.newValue(ChildView.class));
            assertInstanceOf(InternalChildView.class, ctx.newValue(InternalChildView.class));

            assertNull(ctx.newName(ChildView.class));
            assertNull(ctx.newName(InternalChildView.class));

            return nullCompletedFuture();
        });

        config.child().str().update(randomUuid()).get(1, SECONDS);
    }

    @Test
    void testNotificationEventConfigForNamedConfiguration() throws Exception {
        config.children().listenElements(new ConfigurationNamedListListener<>() {
            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<ChildView> ctx) {
                assertInstanceOf(ChildView.class, ctx.newValue(ChildView.class));
                assertInstanceOf(InternalChildView.class, ctx.newValue(InternalChildView.class));

                assertEquals("0", ctx.newName(ChildView.class));
                assertEquals("0", ctx.newName(InternalChildView.class));

                return nullCompletedFuture();
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onRename(ConfigurationNotificationEvent<ChildView> ctx) {
                assertInstanceOf(ChildView.class, ctx.newValue(ChildView.class));
                assertInstanceOf(InternalChildView.class, ctx.newValue(InternalChildView.class));

                assertEquals("1", ctx.newName(ChildView.class));
                assertEquals("1", ctx.newName(InternalChildView.class));

                return nullCompletedFuture();
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<ChildView> ctx) {
                assertNull(ctx.newValue(ChildView.class));
                assertNull(ctx.newValue(InternalChildView.class));

                assertEquals("1", ctx.oldName(ChildView.class));
                assertEquals("1", ctx.oldName(InternalChildView.class));

                assertNull(ctx.newName(ChildView.class));
                assertNull(ctx.newName(InternalChildView.class));

                return nullCompletedFuture();
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<ChildView> ctx) {
                assertInstanceOf(ChildView.class, ctx.newValue(ChildView.class));
                assertInstanceOf(InternalChildView.class, ctx.newValue(InternalChildView.class));

                assertEquals("1", ctx.newName(ChildView.class));
                assertEquals("1", ctx.newName(InternalChildView.class));

                return nullCompletedFuture();
            }
        });

        config.children().change(c -> c.create("0", c1 -> {})).get(1, SECONDS);
        config.children().change(c -> c.rename("0", "1")).get(1, SECONDS);
        config.children().change(c -> c.update("1", c1 -> c1.changeStr(randomUuid()))).get(1, SECONDS);
        config.children().change(c -> c.delete("1")).get(1, SECONDS);
    }

    @Test
    void testNotificationEventConfigForNestedPolymorphicConfiguration() throws Exception {
        config.polyChild().listen(ctx -> {
            assertInstanceOf(PolyView.class, ctx.newValue(PolyView.class));
            assertInstanceOf(StringPolyView.class, ctx.newValue(StringPolyView.class));

            assertNull(ctx.newValue(LongPolyView.class));

            assertNull(ctx.newName(PolyView.class));
            assertNull(ctx.newName(StringPolyView.class));
            assertNull(ctx.newName(LongPolyView.class));

            return nullCompletedFuture();
        });

        config.polyChild().commonIntVal().update(22).get(1, SECONDS);
    }

    @Test
    void testNotificationEventConfigForNamedPolymorphicConfiguration() throws Exception {
        config.polyChildren().listenElements(new ConfigurationNamedListListener<>() {
            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<PolyView> ctx) {
                assertInstanceOf(PolyView.class, ctx.newValue(PolyView.class));
                assertInstanceOf(StringPolyView.class, ctx.newValue(StringPolyView.class));

                assertNull(ctx.newValue(LongPolyView.class));

                assertEquals("0", ctx.newName(PolyView.class));
                assertEquals("0", ctx.newName(StringPolyView.class));

                assertNull(ctx.newName(LongPolyView.class));

                return nullCompletedFuture();
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onRename(ConfigurationNotificationEvent<PolyView> ctx) {
                assertInstanceOf(PolyView.class, ctx.newValue(PolyView.class));
                assertInstanceOf(StringPolyView.class, ctx.newValue(StringPolyView.class));

                assertNull(ctx.newValue(LongPolyView.class));

                assertEquals("1", ctx.newName(PolyView.class));
                assertEquals("1", ctx.newName(StringPolyView.class));

                assertNull(ctx.newName(LongPolyView.class));

                return nullCompletedFuture();
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<PolyView> ctx) {
                assertNull(ctx.newValue(PolyView.class));
                assertNull(ctx.newValue(StringPolyView.class));
                assertNull(ctx.newValue(LongPolyView.class));

                assertNull(ctx.newName(PolyView.class));
                assertNull(ctx.newName(StringPolyView.class));

                assertEquals("1", ctx.oldName(PolyView.class));
                assertEquals("1", ctx.oldName(StringPolyView.class));

                assertNull(ctx.newName(LongPolyView.class));

                return nullCompletedFuture();
            }

            /** {@inheritDoc} */
            @Override
            public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<PolyView> ctx) {
                assertInstanceOf(PolyView.class, ctx.newValue(PolyView.class));
                assertInstanceOf(StringPolyView.class, ctx.newValue(StringPolyView.class));

                assertNull(ctx.newValue(LongPolyView.class));

                assertEquals("1", ctx.newName(PolyView.class));
                assertEquals("1", ctx.newName(StringPolyView.class));

                assertNull(ctx.newName(LongPolyView.class));

                return nullCompletedFuture();
            }
        });

        config.polyChildren().change(c -> c.create("0", c1 -> {})).get(1, SECONDS);
        config.polyChildren().change(c -> c.rename("0", "1")).get(1, SECONDS);
        config.polyChildren().change(c -> c.update("1", c1 -> c1.changeCommonIntVal(22))).get(1, SECONDS);
        config.polyChildren().change(c -> c.delete("1")).get(1, SECONDS);
    }

    @Test
    void testNotificationListenerForNestedPolymorphicConfig() throws Exception {
        AtomicBoolean invokeListener = new AtomicBoolean();

        config.polyChild().listen(configListener(ctx -> {
            invokeListener.set(true);

            assertInstanceOf(PolyView.class, ctx.newValue());
            assertInstanceOf(LongPolyView.class, ctx.newValue());

            assertInstanceOf(PolyView.class, ctx.oldValue());
            assertInstanceOf(StringPolyView.class, ctx.oldValue());

            assertInstanceOf(PolyView.class, ctx.newValue(PolyView.class));
            assertInstanceOf(LongPolyView.class, ctx.newValue(LongPolyView.class));

            assertNull(ctx.newValue(StringPolyView.class));

            assertNull(ctx.newName(PolyView.class));
            assertNull(ctx.newName(LongPolyView.class));
            assertNull(ctx.newName(StringPolyView.class));
        }));

        config.polyChild()
                .change(c -> c.convert(LongPolyChange.class).changeSpecificVal(0).changeCommonIntVal(0))
                .get(1, SECONDS);

        assertTrue(invokeListener.get());
    }

    @Test
    void testNotificationListenerOnCreateNamedPolymorphicConfig() throws Exception {
        AtomicBoolean invokeListener = new AtomicBoolean();

        config.polyChildren().listenElements(configNamedListenerOnCreate(ctx -> {
            invokeListener.set(true);

            assertInstanceOf(PolyView.class, ctx.newValue());
            assertInstanceOf(StringPolyView.class, ctx.newValue());

            assertNull(ctx.oldValue());

            assertInstanceOf(PolyView.class, ctx.newValue(PolyView.class));
            assertInstanceOf(StringPolyView.class, ctx.newValue(StringPolyView.class));

            assertNull(ctx.newValue(LongPolyView.class));

            assertEquals("0", ctx.newName(PolyView.class));
            assertEquals("0", ctx.newName(StringPolyView.class));

            assertNull(ctx.newName(LongPolyView.class));
        }));

        config.polyChildren()
                .change(c -> c.create("0", c1 -> c1.convert(StringPolyChange.class).changeSpecificVal("").changeCommonIntVal(0)))
                .get(1, SECONDS);

        assertTrue(invokeListener.get());
    }

    @Test
    void testNotificationListenerOnUpdateNamedPolymorphicConfig() throws Exception {
        config.polyChildren()
                .change(c -> c.create("0", c1 -> c1.convert(StringPolyChange.class).changeSpecificVal("").changeCommonIntVal(0)))
                .get(1, SECONDS);

        AtomicBoolean invokeListener = new AtomicBoolean();

        config.polyChildren().listenElements(configNamedListenerOnUpdate(ctx -> {
            invokeListener.set(true);

            assertInstanceOf(PolyView.class, ctx.newValue());
            assertInstanceOf(LongPolyView.class, ctx.newValue());

            assertInstanceOf(PolyView.class, ctx.oldValue());
            assertInstanceOf(StringPolyView.class, ctx.oldValue());

            assertInstanceOf(PolyView.class, ctx.newValue(PolyView.class));
            assertInstanceOf(LongPolyView.class, ctx.newValue(LongPolyView.class));

            assertNull(ctx.newValue(StringPolyView.class));

            assertEquals("0", ctx.newName(PolyView.class));
            assertEquals("0", ctx.newName(LongPolyView.class));

            assertNull(ctx.newName(StringPolyView.class));
        }));

        config.polyChildren()
                .change(c -> c.update("0", c1 -> c1.convert(LongPolyChange.class).changeSpecificVal(0).changeCommonIntVal(0)))
                .get(1, SECONDS);

        assertTrue(invokeListener.get());
    }

    @Test
    void testNotificationListenerOnRenameNamedPolymorphicConfig() throws Exception {
        config.polyChildren()
                .change(c -> c.create("0", c1 -> c1.convert(StringPolyChange.class).changeSpecificVal("").changeCommonIntVal(0)))
                .get(1, SECONDS);

        AtomicBoolean invokeListener = new AtomicBoolean();

        config.polyChildren().listenElements(configNamedListenerOnRename(ctx -> {
            invokeListener.set(true);

            assertInstanceOf(PolyView.class, ctx.newValue());
            assertInstanceOf(StringPolyView.class, ctx.newValue());

            assertInstanceOf(PolyView.class, ctx.oldValue());
            assertInstanceOf(StringPolyView.class, ctx.oldValue());

            assertInstanceOf(PolyView.class, ctx.newValue(PolyView.class));
            assertInstanceOf(StringPolyView.class, ctx.newValue(StringPolyView.class));

            assertNull(ctx.newValue(LongPolyView.class));

            assertEquals("1", ctx.newName(PolyView.class));
            assertEquals("1", ctx.newName(StringPolyView.class));

            assertNull(ctx.newName(LongPolyView.class));
        }));

        config.polyChildren()
                .change(c -> c.rename("0", "1"))
                .get(1, SECONDS);

        assertTrue(invokeListener.get());
    }

    @Test
    void testNotificationListenerOnDeleteNamedPolymorphicConfig() throws Exception {
        config.polyChildren()
                .change(c -> c.create("0", c1 -> c1.convert(StringPolyChange.class).changeSpecificVal("").changeCommonIntVal(0)))
                .get(1, SECONDS);

        AtomicBoolean invokeListener = new AtomicBoolean();

        config.polyChildren().listenElements(configNamedListenerOnDelete(ctx -> {
            invokeListener.set(true);

            assertNull(ctx.newValue());

            assertInstanceOf(PolyView.class, ctx.oldValue());
            assertInstanceOf(StringPolyView.class, ctx.oldValue());

            assertNull(ctx.newValue(PolyView.class));
            assertNull(ctx.newValue(StringPolyView.class));
            assertNull(ctx.newValue(LongPolyView.class));

            assertNull(ctx.newName(PolyView.class));
            assertNull(ctx.newName(StringPolyView.class));

            assertEquals("0", ctx.oldName(PolyView.class));
            assertEquals("0", ctx.oldName(StringPolyView.class));

            assertNull(ctx.newName(LongPolyView.class));
        }));

        config.polyChildren()
                .change(c -> c.delete("0"))
                .get(1, SECONDS);

        assertTrue(invokeListener.get());
    }

    @Test
    void testNotificationEventForNestedConfigAfterNotifyListeners() throws Exception {
        AtomicReference<ConfigurationNotificationEvent<?>> eventRef = new AtomicReference<>();

        config.child().listen(configListener(eventRef::set));

        config.child().str().update(randomUuid()).get(1, SECONDS);

        ConfigurationNotificationEvent<?> event = eventRef.get();

        assertNotNull(event);

        assertInstanceOf(ChildView.class, event.newValue());
        assertInstanceOf(InternalChildView.class, event.newValue());

        assertInstanceOf(ChildView.class, event.oldValue());
        assertInstanceOf(InternalChildView.class, event.oldValue());

        assertInstanceOf(ChildView.class, event.newValue(ChildView.class));
        assertInstanceOf(InternalChildView.class, event.newValue(InternalChildView.class));

        assertInstanceOf(ParentView.class, event.newValue(ParentView.class));

        assertNull(event.newName(ChildView.class));
        assertNull(event.newName(InternalChildView.class));
    }

    @Test
    void testNotificationEventForNamedConfigAfterNotifyListeners() throws Exception {
        AtomicReference<ConfigurationNotificationEvent<?>> eventRef = new AtomicReference<>();

        config.children().listenElements(configNamedListenerOnCreate(eventRef::set));

        config.children().change(c -> c.create("0", doNothingConsumer())).get(1, SECONDS);

        ConfigurationNotificationEvent<?> event = eventRef.get();

        assertNotNull(event);

        assertInstanceOf(ChildView.class, event.newValue());
        assertInstanceOf(InternalChildView.class, event.newValue());

        assertNull(event.oldValue());

        assertInstanceOf(ChildView.class, event.newValue(ChildView.class));
        assertInstanceOf(InternalChildView.class, event.newValue(InternalChildView.class));

        assertInstanceOf(ParentView.class, event.newValue(ParentView.class));

        assertEquals("0", event.newName(ChildView.class));
        assertEquals("0", event.newName(InternalChildView.class));
    }

    @Test
    void testGetErrorFromListener() {
        config.child().listen(configListener(ctx -> {
            throw new RuntimeException("from test");
        }));

        ExecutionException ex = assertThrows(
                ExecutionException.class,
                () -> config.child().str().update(randomUuid()).get(1, SECONDS)
        );

        assertTrue(hasCause(ex, RuntimeException.class, "from test"));
    }

    @Test
    void testGetErrorFromListenerFuture() {
        config.child().listen(ctx -> failedFuture(new RuntimeException("from test")));

        ExecutionException ex = assertThrows(
                ExecutionException.class,
                () -> config.child().str().update(randomUuid()).get(1, SECONDS)
        );

        assertTrue(hasCause(ex, RuntimeException.class, "from test"));
    }

    @Test
    void testNotifyListenersOnCurrentConfigWithoutChange() throws Exception {
        config.children().change(c -> c.create("0", doNothingConsumer())).get(1, SECONDS);

        config.polyChildren().change(c -> c.create("0", doNothingConsumer())).get(1, SECONDS);

        List<String> events = new ArrayList<>();

        config.listen(configListener(ctx -> events.add("root")));

        config.child().listen(configListener(ctx -> events.add("child")));
        config.child().str().listen(configListener(ctx -> events.add("child.str")));

        config.children().listen(configListener(ctx -> events.add("children")));
        config.children().listenElements(configNamedListenerOnCreate(ctx -> events.add("children.onCreate")));
        config.children().listenElements(configNamedListenerOnUpdate(ctx -> events.add("children.onUpdate")));
        config.children().listenElements(configNamedListenerOnRename(ctx -> events.add("children.onRename")));
        config.children().listenElements(configNamedListenerOnDelete(ctx -> events.add("children.onDelete")));

        config.children().get("0").listen(configListener(ctx -> events.add("children.0")));
        config.children().get("0").str().listen(configListener(ctx -> events.add("children.0.str")));

        config.children().any().listen(configListener(ctx -> events.add("children.any")));
        config.children().any().str().listen(configListener(ctx -> events.add("children.any.str")));

        // Polymorphic configs.

        config.polyChild().listen(configListener(ctx -> events.add("polyChild")));
        config.polyChild().commonIntVal().listen(configListener(ctx -> events.add("polyChild.int")));
        ((StringPolyConfiguration) config.polyChild()).specificVal().listen(configListener(ctx -> events.add("polyChild.str")));

        config.polyChildren().listen(configListener(ctx -> events.add("polyChildren")));
        config.polyChildren().listenElements(configNamedListenerOnCreate(ctx -> events.add("polyChildren.onCreate")));
        config.polyChildren().listenElements(configNamedListenerOnUpdate(ctx -> events.add("polyChildren.onUpdate")));
        config.polyChildren().listenElements(configNamedListenerOnRename(ctx -> events.add("polyChildren.onRename")));
        config.polyChildren().listenElements(configNamedListenerOnDelete(ctx -> events.add("polyChildren.onDelete")));

        config.polyChildren().get("0").listen(configListener(ctx -> events.add("polyChildren.0")));
        config.polyChildren().get("0").commonIntVal().listen(configListener(ctx -> events.add("polyChildren.0.int")));
        ((StringPolyConfiguration) config.polyChildren().get("0")).specificVal()
                .listen(configListener(ctx -> events.add("polyChildren.0.str")));

        config.polyChildren().any().listen(configListener(ctx -> events.add("polyChildren.any")));
        config.polyChildren().any().commonIntVal().listen(configListener(ctx -> events.add("polyChildren.any.int")));

        Collection<CompletableFuture<?>> futs = notifyListeners(
                null,
                (InnerNode) config.value(),
                (DynamicConfiguration) config,
                0,
                registry.notificationCount() + 1
        );

        for (CompletableFuture<?> fut : futs) {
            fut.get(1, SECONDS);
        }

        assertEquals(
                List.of(
                        "root",
                        "child", "child.str",
                        "children", "children.onCreate", "children.any", "children.0",
                        "children.any.str", "children.0.str",
                        "polyChild", "polyChild.int", "polyChild.str",
                        "polyChildren", "polyChildren.onCreate", "polyChildren.any", "polyChildren.0",
                        "polyChildren.any.int", "polyChildren.0.int", "polyChildren.0.str"
                ),
                events
        );
    }

    @Test
    void testNotifyCurrentConfigurationListeners() throws Exception {
        AtomicBoolean invokeListener = new AtomicBoolean();

        config.listen(configListener(ctx -> {
            invokeListener.set(true);

            assertNull(ctx.oldValue());
            assertNotNull(ctx.newValue());
        }));

        registry.notifyCurrentConfigurationListeners().get(1, SECONDS);

        assertTrue(invokeListener.get());
    }

    @Test
    void testIncreaseNotificationCount() throws Exception {
        long notificationCount = registry.notificationCount();

        assertTrue(notificationCount >= 0);

        config.child().str().update(randomUuid()).get(1, SECONDS);

        assertEquals(notificationCount + 1, registry.notificationCount());

        registry.notifyCurrentConfigurationListeners().get(1, SECONDS);

        assertEquals(notificationCount + 2, registry.notificationCount());
    }

    @Test
    void testGenerateNotificationWhenUpdateWithCurrentValue() throws Exception {
        long notificationCount = registry.notificationCount();

        assertTrue(notificationCount >= 0);

        String currentValue = config.child().str().value();
        config.child().str().update(currentValue).get(1, SECONDS);

        assertEquals(notificationCount + 1, registry.notificationCount());

        registry.notifyCurrentConfigurationListeners().get(1, SECONDS);

        assertEquals(notificationCount + 2, registry.notificationCount());
    }

    @Test
    void testNotifyListenersOnNextUpdateConfiguration() throws Exception {
        List<String> events = new ArrayList<>();

        AtomicBoolean addListeners = new AtomicBoolean(true);

        config.listen(configListener(ctx0 -> {
            events.add("root");

            if (addListeners.get()) {
                config.child().listen(configListener(ctx1 -> events.add("child")));

                config.child().str().listen(configListener(ctx1 -> events.add("child.str")));

                config.children().listen(configListener(ctx1 -> events.add("children")));

                config.children().listenElements(configNamedListenerOnCreate(ctx1 -> events.add("children.onCreate")));

                config.children().listenElements(configNamedListenerOnUpdate(ctx1 -> events.add("children.onUpdate")));

                config.children().get("0").listen(configListener(ctx1 -> events.add("children.0")));

                config.children().get("0").str().listen(configListener(ctx1 -> events.add("children.0.str")));

                config.children().any().listen(configListener(ctx1 -> events.add("children.any")));

                config.children().any().str().listen(configListener(ctx1 -> events.add("children.any.str")));

                addListeners.set(false);
            }
        }));

        config.change(
                c0 -> c0.changeChild(c1 -> c1.changeStr(randomUuid())).changeChildren(c1 -> c1.create("0", doNothingConsumer()))
        ).get(1, SECONDS);

        assertEquals(List.of("root"), events);

        assertFalse(addListeners.get());

        events.clear();

        config.change(c0 -> c0.changeChild(c1 -> c1.changeStr(randomUuid()))
                .changeChildren(c1 -> c1.create("1", doNothingConsumer()).update("0", c2 -> c2.changeStr(randomUuid())))
        ).get(1, SECONDS);

        assertEquals(
                List.of(
                        "root",
                        "child", "child.str",
                        "children",
                        "children.onCreate", "children.any", "children.any.str",
                        "children.onUpdate", "children.any", "children.0", "children.any.str", "children.0.str"
                ),
                events
        );

        assertFalse(addListeners.get());
    }
}
