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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.configuration.ConfigurationRegistry;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

/** */
public class ConfigurationListenerTest {
    /** */
    @ConfigurationRoot(rootName = "parent", type = ConfigurationType.LOCAL)
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
    }

    /** */
    private ConfigurationRegistry registry;

    /** */
    private ParentConfiguration configuration;

    /** */
    @BeforeEach
    public void before() {
        ConfigurationStorage testConfigurationStorage = new TestConfigurationStorage();

        registry = new ConfigurationRegistry(
            Collections.singletonList(ParentConfiguration.KEY),
            Collections.emptyMap(),
            Collections.singletonList(testConfigurationStorage)
        );

        registry.startStorageConfigurations(testConfigurationStorage.type());

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
        List<String> log = new CopyOnWriteArrayList<>();

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

    /** */
    @Test
    public void namedListNode() throws Exception {
        List<String> log = new CopyOnWriteArrayList<>();

        configuration.listen(ctx -> {
            log.add("parent");

            return completedFuture(null);
        });

        configuration.child().listen(ctx -> {
            log.add("child");

            return completedFuture(null);
        });

        configuration.elements().listen(ctx -> {
            if (ctx.oldValue().size() == 0) {
                ChildView newValue = ctx.newValue().get("name");

                assertNotNull(newValue);
                assertEquals("default", newValue.str());
            }
            else if (ctx.newValue().size() == 0) {
                ChildView oldValue = ctx.oldValue().get("newName");

                assertNotNull(oldValue);
                assertEquals("foo", oldValue.str());
            }
            else if (!ctx.newValue().namedListKeys().contains("newName")) {
                ChildView oldValue = ctx.oldValue().get("name");

                assertNotNull(oldValue);
                assertEquals("default", oldValue.str());

                ChildView newValue = ctx.newValue().get("name");

                assertNotNull(newValue);
                assertEquals("foo", newValue.str());
            }
            else {
                assertEquals(1, ctx.oldValue().size());

                ChildView oldValue = ctx.oldValue().get("name");

                assertNotNull(oldValue);
                assertEquals("foo", oldValue.str());

                assertEquals(1, ctx.newValue().size());

                ChildView newValue = ctx.newValue().get("newName");

                assertNotNull(newValue);
                assertEquals("foo", newValue.str());

                assertNotSame(oldValue, newValue); // Sadly, there's no reference equality invariant on rename.
            }

            log.add("elements");

            return completedFuture(null);
        });

        configuration.elements().listen(new ConfigurationNamedListListener<ChildView>() {
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
            @Override public @NotNull CompletableFuture<?> onRename(
                String oldName,
                String newName,
                ConfigurationNotificationEvent<ChildView> ctx
            ) {
                assertEquals("name", oldName);
                assertEquals("newName", newName);

                ChildView oldValue = ctx.oldValue();

                assertNotNull(oldValue);
                assertEquals("foo", oldValue.str());

                ChildView newValue = ctx.newValue();

                assertNotNull(newValue);
                assertEquals("foo", newValue.str());

                assertNotSame(oldValue, newValue);

                log.add("rename");

                return completedFuture(null);
            }

            /** {@inheritDoc} */
            @Override public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<ChildView> ctx) {
                assertNull(ctx.newValue());

                ChildView oldValue = ctx.oldValue();

                assertNotNull(oldValue);
                assertEquals("foo", oldValue.str());

                log.add("delete");

                return completedFuture(null);
            }
        });

        configuration.change(parent ->
            parent.changeElements(elements -> elements.create("name", element -> {}))
        ).get(1, SECONDS);

        assertEquals(List.of("parent", "elements", "create"), log);

        log.clear();

        configuration.change(parent ->
            parent.changeElements(elements -> elements.createOrUpdate("name", element -> element.changeStr("foo")))
        ).get(1, SECONDS);

        assertEquals(List.of("parent", "elements", "update"), log);

        log.clear();

        configuration.change(parent ->
            parent.changeElements(elements -> elements.rename("name", "newName"))
        ).get(1, SECONDS);

        assertEquals(List.of("parent", "elements", "rename"), log);

        log.clear();

        configuration.change(parent ->
            parent.changeElements(elements -> elements.delete("newName"))
        ).get(1, SECONDS);

        assertEquals(List.of("parent", "elements", "delete"), log);
    }

    /** */
    @Test
    @Disabled("Will be fixed in https://issues.apache.org/jira/browse/IGNITE-15193")
    public void dataRace() throws Exception {
        configuration.change(parent -> parent.changeElements(elements ->
            elements.create("name", e -> {}))
        ).get(1, SECONDS);

        CountDownLatch wait = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        List<String> log = new CopyOnWriteArrayList<>();

        configuration.listen(ctx -> {
            try {
                wait.await();
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

        release.await();

        fut.get(1, SECONDS);

        assertEquals(List.of("deleted"), log);
    }
}
