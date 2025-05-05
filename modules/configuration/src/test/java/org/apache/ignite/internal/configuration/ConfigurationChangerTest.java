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

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.configuration.FirstConfiguration.KEY;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import com.typesafe.config.ConfigFactory;
import java.io.Serializable;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.ignite.configuration.ConfigurationChangeException;
import org.apache.ignite.configuration.NamedListView;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Immutable;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;
import org.apache.ignite.internal.configuration.direct.KeyPathNode;
import org.apache.ignite.internal.configuration.hocon.HoconConverter;
import org.apache.ignite.internal.configuration.storage.ConfigurationStorage;
import org.apache.ignite.internal.configuration.storage.Data;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.tree.ConfigurationSource;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNode;
import org.apache.ignite.internal.configuration.validation.ConfigurationValidatorImpl;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

/**
 * Test configuration changer.
 */
public class ConfigurationChangerTest {
    /** Annotation used to test failing validation. */
    @Target(FIELD)
    @Retention(RUNTIME)
    @interface MaybeInvalid {
    }

    /**
     * First configuration schema.
     */
    @ConfigurationRoot(rootName = "key", type = LOCAL)
    public static class FirstConfigurationSchema {
        @ConfigValue
        @MaybeInvalid
        public SecondConfigurationSchema child;

        @NamedConfigValue
        public ThirdConfigurationSchema elements;
    }

    /**
     * Second configuration schema.
     */
    @Config
    public static class SecondConfigurationSchema {
        @Value(hasDefault = true)
        @Immutable
        public int intCfg = 0;

        @Value(hasDefault = true)
        public String strCfg = "";
    }

    /**
     * Third configuration schema.
     */
    @Config
    public static class ThirdConfigurationSchema {
        @Value
        public String strCfg;
    }

    private static ConfigurationTreeGenerator generator = new ConfigurationTreeGenerator(KEY, DefaultsConfiguration.KEY);

    private final TestConfigurationStorage storage = new TestConfigurationStorage(LOCAL);

    @AfterAll
    public static void afterAll() {
        generator.close();
    }

    /**
     * Test simple change of configuration.
     */
    @Test
    public void testSimpleConfigurationChange() throws Exception {
        ConfigurationChanger changer = createChanger(KEY);
        changer.start();

        changer.change(source(KEY, (FirstChange parent) -> parent
                .changeChild(change -> change.changeIntCfg(1).changeStrCfg("1"))
                .changeElements(change -> change.create("a", element -> element.changeStrCfg("1")))
        )).get(1, SECONDS);

        FirstView newRoot = (FirstView) changer.getRootNode(KEY);

        assertEquals(1, newRoot.child().intCfg());
        assertEquals("1", newRoot.child().strCfg());
        assertEquals("1", newRoot.elements().get("a").strCfg());
    }

    /**
     * Test simple change of configuration.
     */
    @Test
    public void testSimpleConfigurationChangeWithoutExtraLambdas() throws Exception {
        ConfigurationChanger changer = createChanger(KEY);
        changer.start();

        changer.change(source(KEY, (FirstChange parent) -> {
            parent.changeChild().changeIntCfg(1).changeStrCfg("1");
            parent.changeElements().create("a", element -> element.changeStrCfg("1"));
        })).get(1, SECONDS);

        FirstView newRoot = (FirstView) changer.getRootNode(KEY);

        assertEquals(1, newRoot.child().intCfg());
        assertEquals("1", newRoot.child().strCfg());
        assertEquals("1", newRoot.elements().get("a").strCfg());
    }

    /**
     * Test subsequent change of configuration via different changers.
     */
    @Test
    public void testModifiedFromAnotherStorage() throws Exception {
        ConfigurationChanger changer1 = createChanger(KEY);
        changer1.start();

        ConfigurationChanger changer2 = createChanger(KEY);
        changer2.start();

        changer1.change(source(KEY, (FirstChange parent) -> parent
                .changeChild(change -> change.changeIntCfg(1).changeStrCfg("1"))
                .changeElements(change -> change.create("a", element -> element.changeStrCfg("1")))
        )).get(1, SECONDS);

        changer2.change(source(KEY, (FirstChange parent) -> parent
                .changeChild(change -> change.changeIntCfg(2).changeStrCfg("2"))
                .changeElements(change -> change
                        .createOrUpdate("a", element -> element.changeStrCfg("2"))
                        .create("b", element -> element.changeStrCfg("2"))
                )
        )).get(1, SECONDS);

        FirstView newRoot1 = (FirstView) changer1.getRootNode(KEY);

        assertEquals(2, newRoot1.child().intCfg());
        assertEquals("2", newRoot1.child().strCfg());
        assertEquals("2", newRoot1.elements().get("a").strCfg());
        assertEquals("2", newRoot1.elements().get("b").strCfg());

        FirstView newRoot2 = (FirstView) changer2.getRootNode(KEY);

        assertEquals(2, newRoot2.child().intCfg());
        assertEquals("2", newRoot2.child().strCfg());
        assertEquals("2", newRoot2.elements().get("a").strCfg());
        assertEquals("2", newRoot2.elements().get("b").strCfg());
    }

    /**
     * Test that subsequent change of configuration is failed if changes are incompatible.
     */
    @Test
    public void testModifiedFromAnotherStorageWithIncompatibleChanges() throws Exception {
        ConfigurationChanger changer1 = createChanger(KEY);
        changer1.start();

        Validator<MaybeInvalid, Object> validator = new Validator<>() {
            /** {@inheritDoc} */
            @Override
            public void validate(MaybeInvalid annotation, ValidationContext<Object> ctx) {
                if (ctx.getNewValue().equals("2")) {
                    ctx.addIssue(new ValidationIssue("key", "foo"));
                }
            }
        };

        ConfigurationChanger changer2 = new TestConfigurationChanger(
                List.of(KEY),
                storage,
                generator,
                ConfigurationValidatorImpl.withDefaultValidators(generator, Set.of(validator))
        );

        changer2.start();

        changer1.change(source(KEY, (FirstChange parent) -> parent
                .changeChild(change -> change.changeIntCfg(1).changeStrCfg("1"))
                .changeElements(change -> change.create("a", element -> element.changeStrCfg("1")))
        )).get(1, SECONDS);

        assertThrows(ExecutionException.class, () -> changer2.change(source(KEY, (FirstChange parent) -> parent
                .changeChild(change -> change.changeIntCfg(2).changeStrCfg("2"))
                .changeElements(change -> change
                        .create("a", element -> element.changeStrCfg("2"))
                        .create("b", element -> element.changeStrCfg("2"))
                )
        )).get(1, SECONDS));

        FirstView newRoot = (FirstView) changer2.getRootNode(KEY);

        assertEquals(1, newRoot.child().intCfg());
        assertEquals("1", newRoot.child().strCfg());
        assertEquals("1", newRoot.elements().get("a").strCfg());
    }

    /**
     * Test that change fails with right exception if storage is inaccessible.
     */
    @Test
    public void testFailedToWrite() {
        ConfigurationChanger changer = createChanger(KEY);

        storage.fail(true);

        assertThrows(ConfigurationChangeException.class, changer::start);

        storage.fail(false);

        changer.start();

        storage.fail(true);

        assertThrows(ExecutionException.class, () -> changer.change(source(KEY, (FirstChange parent) -> parent
                .changeChild(child -> child.changeIntCfg(1).changeStrCfg("1"))
        )).get(1, SECONDS));

        storage.fail(false);

        CompletableFuture<Map<String, ? extends Serializable>> dataFuture = storage.readDataOnRecovery().thenApply(Data::values);

        assertThat(dataFuture, willCompleteSuccessfully());

        FirstView newRoot = (FirstView) changer.getRootNode(KEY);
        assertNotNull(newRoot.child());
        assertThat(newRoot.child().strCfg(), is(emptyString()));
    }

    /**
     * Configuration schema with default values for children.
     */
    @ConfigurationRoot(rootName = "def", type = LOCAL)
    public static class DefaultsConfigurationSchema {
        @ConfigValue
        public DefaultsChildConfigurationSchema child;

        @NamedConfigValue
        public DefaultsChildConfigurationSchema childrenList;

        @Value(hasDefault = true)
        public String defStr = "foo";
    }

    /**
     * Configuration schema with default values for children.
     */
    @Config
    public static class DefaultsChildConfigurationSchema {
        @Value(hasDefault = true)
        public String defStr = "bar";

        @Value(hasDefault = true)
        public String[] arr = {"xyz"};
    }

    @Test
    public void defaultsOnInit() throws Exception {
        var changer = createChanger(DefaultsConfiguration.KEY);

        changer.start();

        DefaultsView root = (DefaultsView) changer.getRootNode(DefaultsConfiguration.KEY);

        assertEquals("foo", root.defStr());
        assertEquals("bar", root.child().defStr());
        assertEquals(List.of("xyz"), Arrays.asList(root.child().arr()));

        changer.change(source(DefaultsConfiguration.KEY, (DefaultsChange def) -> def
                .changeChildrenList(children ->
                        children.create("name", child -> {
                        })
                )
        )).get(1, SECONDS);

        root = (DefaultsView) changer.getRootNode(DefaultsConfiguration.KEY);

        assertEquals("bar", root.childrenList().get("name").defStr());
    }

    /**
     * Tests the {@link DynamicConfigurationChanger#getLatest} method by retrieving different configuration values.
     */
    @Test
    public void testGetLatest() throws Exception {
        ConfigurationChanger changer = createChanger(DefaultsConfiguration.KEY);

        changer.start();

        ConfigurationSource source = source(
                DefaultsConfiguration.KEY,
                (DefaultsChange change) -> change.changeChildrenList(children -> children.create("name", child -> {
                }))
        );

        changer.change(source).get(1, SECONDS);

        DefaultsView configurationView = changer.getLatest(List.of(node("def")));

        assertEquals("foo", configurationView.defStr());
        assertEquals("bar", configurationView.child().defStr());
        assertArrayEquals(new String[]{"xyz"}, configurationView.child().arr());
        assertEquals("bar", configurationView.childrenList().get("name").defStr());
    }

    /**
     * Tests the {@link DynamicConfigurationChanger#getLatest} method by retrieving different nested configuration values.
     */
    @Test
    public void testGetLatestNested() {
        ConfigurationChanger changer = createChanger(DefaultsConfiguration.KEY);

        changer.start();

        DefaultsChildView childView = changer.getLatest(List.of(node("def"), node("child")));

        assertEquals("bar", childView.defStr());
        assertArrayEquals(new String[]{"xyz"}, childView.arr());

        String childStrValueView = changer.getLatest(List.of(node("def"), node("child"), node("defStr")));

        assertEquals("bar", childStrValueView);

        String[] childArrView = changer.getLatest(List.of(node("def"), node("child"), node("arr")));

        assertArrayEquals(new String[]{"xyz"}, childArrView);
    }

    /**
     * Tests the {@link DynamicConfigurationChanger#getLatest} method by retrieving different Named List configuration values.
     */
    @Test
    public void testGetLatestNamedList() throws Exception {
        ConfigurationChanger changer = createChanger(DefaultsConfiguration.KEY);

        changer.start();

        ConfigurationSource source = source(
                DefaultsConfiguration.KEY,
                (DefaultsChange change) -> change.changeChildrenList(children -> {
                    children.create("name1", child -> {
                    });
                    children.create("name2", child -> {
                    });
                })
        );

        changer.change(source).get(1, SECONDS);

        for (String name : List.of("name1", "name2")) {
            NamedListView<DefaultsChildView> childrenListView = changer.getLatest(List.of(node("def"), node("childrenList")));

            DefaultsChildView childrenListElementView = childrenListView.get(name);

            assertEquals("bar", childrenListElementView.defStr());
            assertArrayEquals(new String[]{"xyz"}, childrenListElementView.arr());

            childrenListElementView = changer.getLatest(List.of(node("def"), node("childrenList"), listNode(name)));

            assertEquals("bar", childrenListElementView.defStr());
            assertArrayEquals(new String[]{"xyz"}, childrenListElementView.arr());

            String childrenListStrValueView = changer.getLatest(
                    List.of(node("def"), node("childrenList"), listNode(name), node("defStr"))
            );

            assertEquals("bar", childrenListStrValueView);

            String[] childrenListArrView = changer.getLatest(
                    List.of(node("def"), node("childrenList"), listNode(name), node("arr"))
            );

            assertArrayEquals(new String[]{"xyz"}, childrenListArrView);
        }
    }

    /**
     * Tests the {@link DynamicConfigurationChanger#getLatest} method by trying to find non-existend values on different configuration
     * levels.
     */
    @Test
    public void testGetLatestMissingKey() throws Exception {
        ConfigurationChanger changer = createChanger(DefaultsConfiguration.KEY);

        changer.start();

        ConfigurationSource source = source(
                DefaultsConfiguration.KEY,
                (DefaultsChange change) -> change.changeChildrenList(children -> children.create("name", child -> {
                }))
        );

        changer.change(source).get(1, SECONDS);

        NoSuchElementException e = assertThrows(NoSuchElementException.class, () -> changer.getLatest(List.of(node("foo"))));

        assertThat(e.getMessage(), containsString("foo"));

        e = assertThrows(NoSuchElementException.class, () -> changer.getLatest(List.of(node("def"), node("foo"))));

        assertThat(e.getMessage(), containsString("foo"));

        e = assertThrows(NoSuchElementException.class, () -> changer.getLatest(List.of(node("def"), node("defStr"), node("foo"))));

        assertThat(e.getMessage(), containsString("def.defStr.foo"));

        e = assertThrows(NoSuchElementException.class, () -> changer.getLatest(List.of(node("def"), node("child"), node("foo"))));

        assertThat(e.getMessage(), containsString("foo"));

        e = assertThrows(
                NoSuchElementException.class,
                () -> changer.getLatest(List.of(node("def"), node("child"), node("defStr"), node("foo")))
        );

        assertThat(e.getMessage(), containsString("def.child.defStr.foo"));

        e = assertThrows(NoSuchElementException.class, () ->
                changer.getLatest(List.of(node("def"), node("childrenList"), listNode("foo")))
        );

        assertThat(e.getMessage(), containsString("def.childrenList.foo"));

        e = assertThrows(
                NoSuchElementException.class,
                () -> changer.getLatest(List.of(node("def"), node("childrenList"), listNode("name"), node("foo")))
        );

        assertThat(e.getMessage(), containsString("def.childrenList.name.foo"));

        e = assertThrows(
                NoSuchElementException.class,
                () -> changer.getLatest(List.of(node("def"), node("childrenList"), listNode("name"), node("defStr"), node("foo")))
        );

        assertThat(e.getMessage(), containsString("def.childrenList.name.defStr"));
    }

    /**
     * Check that {@link ConfigurationStorage#lastRevision} always returns the latest revision of the storage,
     * and that the change will only be applied on the last revision of the storage.
     *
     * @throws Exception If failed.
     */
    @Test
    void testLastRevision() throws Exception {
        ConfigurationChanger changer = createChanger(DefaultsConfiguration.KEY);

        changer.start();

        assertEquals(1, storage.lastRevision().get(1, SECONDS));

        changer.change(source(DefaultsConfiguration.KEY, (DefaultsChange c) -> c.changeDefStr("test0"))).get(1, SECONDS);
        assertEquals(2, storage.lastRevision().get(1, SECONDS));

        // Increase the revision so that the change waits for the latest revision of the configuration to be received from the storage.
        storage.incrementAndGetRevision();
        assertEquals(3, storage.lastRevision().get(1, SECONDS));

        AtomicInteger invokeConsumerCnt = new AtomicInteger();

        CompletableFuture<Void> changeFut = changer.change(source(
                DefaultsConfiguration.KEY,
                (DefaultsChange c) -> {
                    invokeConsumerCnt.incrementAndGet();

                    try {
                        // Let's check that the consumer will be called on the last revision of the repository.
                        assertEquals(3, storage.lastRevision().get(1, SECONDS));
                    } catch (Exception e) {
                        fail(e);
                    }

                    c.changeDefStr("test1");
                }
        ));

        assertThrows(TimeoutException.class, () -> changeFut.get(1, SECONDS));
        assertEquals(0, invokeConsumerCnt.get());

        // Let's roll back the previous revision so that the new change can be applied.
        storage.decrementAndGetRevision();
        assertEquals(2, storage.lastRevision().get(1, SECONDS));

        changer.change(source(DefaultsConfiguration.KEY, (DefaultsChange c) -> c.changeDefStr("test00"))).get(1, SECONDS);

        changeFut.get(1, SECONDS);
        assertEquals(1, invokeConsumerCnt.get());
    }

    /**
     * Checks that if we have not changed the configuration, then the update in the storage will still occur.
     *
     * @throws Exception If failed.
     */
    @Test
    void testUpdateStorageRevisionOnEmptyChanges() throws Exception {
        ConfigurationChanger changer = createChanger(DefaultsConfiguration.KEY);

        changer.start();

        assertEquals(1, storage.lastRevision().get(1, SECONDS));

        changer.change(source(DefaultsConfiguration.KEY, (DefaultsChange c) -> {})).get(1, SECONDS);
        assertEquals(2, storage.lastRevision().get(1, SECONDS));

        changer.change(source(DefaultsConfiguration.KEY, (DefaultsChange c) -> c.changeDefStr("foo"))).get(1, SECONDS);
        assertEquals(3, storage.lastRevision().get(1, SECONDS));
    }

    @Test
    public void initializeWith() throws Exception {
        // When we initialize the configuration with a source, the configuration should be updated with the values from the source.
        String initialConfiguration = "def:{child:{defStr:initialStr,arr:[bar]}}";
        com.typesafe.config.Config config = ConfigFactory.parseString(initialConfiguration);
        ConfigurationSource hoconSource = HoconConverter.hoconSource(config.root());

        ConfigurationChanger changer = createChanger(DefaultsConfiguration.KEY);

        changer.initializeConfigurationWith(hoconSource);

        changer.start();

        DefaultsView root = (DefaultsView) changer.getRootNode(DefaultsConfiguration.KEY);

        assertEquals("foo", root.defStr());
        assertEquals("initialStr", root.child().defStr());
        assertArrayEquals(new String[]{"bar"}, root.child().arr());
    }

    private static <CHANGET> ConfigurationSource source(RootKey<?, ? super CHANGET> rootKey, Consumer<CHANGET> changer) {
        return new ConfigurationSource() {
            @Override
            public void descend(ConstructableTreeNode node) {
                ConfigurationSource changerSrc = new ConfigurationSource() {
                    @Override
                    public void descend(ConstructableTreeNode node) {
                        changer.accept((CHANGET) node);
                    }
                };

                node.construct(rootKey.key(), changerSrc, true);
            }
        };
    }

    private ConfigurationChanger createChanger(RootKey<?, ?> rootKey) {

        return new TestConfigurationChanger(
                List.of(rootKey),
                storage,
                generator,
                new TestConfigurationValidator()
        );
    }

    private static KeyPathNode node(String key) {
        return new KeyPathNode(key);
    }

    private static KeyPathNode listNode(String key) {
        return new KeyPathNode(key, true);
    }
}
