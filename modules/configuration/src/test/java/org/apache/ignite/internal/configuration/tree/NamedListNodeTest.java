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

package org.apache.ignite.internal.configuration.tree;

import static java.lang.String.format;
import static org.apache.ignite.configuration.annotation.ConfigurationType.LOCAL;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.ConfigurationTreeGenerator;
import org.apache.ignite.internal.configuration.TestConfigurationChanger;
import org.apache.ignite.internal.configuration.storage.Data;
import org.apache.ignite.internal.configuration.storage.TestConfigurationStorage;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Test for named list nodes. */
public class NamedListNodeTest {
    /** Root that has a single named list. */
    @ConfigurationRoot(rootName = "a")
    public static class FirstConfigurationSchema {
        @NamedConfigValue
        public SecondConfigurationSchema second;
    }

    /** Named list element node that contains another named list. */
    @Config
    public static class SecondConfigurationSchema {
        /** Every named list element node must have at least one configuration field that is not named list. */
        @Value(hasDefault = true)
        public String str = "foo";

        @NamedConfigValue
        public ThirdConfigurationSchema third;
    }

    /** Simple configuration schema. */
    @Config
    public static class ThirdConfigurationSchema {
        /** Integer value. */
        @Value(hasDefault = true)
        public int intVal = 1;
    }

    /** Runtime implementations generator. */
    private static ConfigurationTreeGenerator cgen;

    /** Test configuration storage. */
    private TestConfigurationStorage storage;

    /** Test configuration changer. */
    private TestConfigurationChanger changer;

    /** Instantiates {@link #cgen}. */
    @BeforeAll
    public static void beforeAll() {
        cgen = new ConfigurationTreeGenerator(FirstConfiguration.KEY);
    }

    /** Nullifies {@link #cgen} to prevent memory leak from having runtime ClassLoader accessible from GC root. */
    @AfterAll
    public static void afterAll() {
        cgen.close();
    }

    /**
     * Before each.
     */
    @BeforeEach
    public void before() {
        storage = new TestConfigurationStorage(LOCAL);

        changer = new TestConfigurationChanger(
                List.of(FirstConfiguration.KEY),
                storage,
                cgen,
                new TestConfigurationValidator()
        );

        changer.start();
    }

    /**
     * After each.
     */
    @AfterEach
    public void after() throws Exception {
        changer.stop();
    }

    /**
     * Tests that there are no unnecessary {@code <order>} values in the storage after all basic named list operations.
     *
     * @throws Exception If failed.
     */
    @Test
    public void storageData() throws Exception {
        // Manually instantiate configuration instance.
        var a = (FirstConfiguration) cgen.instantiateCfg(FirstConfiguration.KEY, changer);

        // Create values on several layers at the same time. They all should have <order> = 0.
        a.second().change(b -> b.create("X", x -> x.changeThird(xb -> xb.create("Z0", z0 -> {})))).get();

        UUID x0Id = ((NamedListNode<?>) a.second().value()).internalId("X");
        UUID z0Id = ((NamedListNode<?>) a.second().get("X").third().value()).internalId("Z0");

        CompletableFuture<Map<String, ? extends Serializable>> storageValues = storage.readDataOnRecovery().thenApply(Data::values);

        assertThat(
                storageValues,
                willBe(Matchers.<Map<String, ? extends Serializable>>allOf(
                        aMapWithSize(8),
                        hasEntry(format("a.second.<ids>.X"), x0Id),
                        hasEntry(format("a.second.%s.str", x0Id), "foo"),
                        hasEntry(format("a.second.%s.<order>", x0Id), 0),
                        hasEntry(format("a.second.%s.<name>", x0Id), "X"),
                        hasEntry(format("a.second.%s.third.<ids>.Z0", x0Id), z0Id),
                        hasEntry(format("a.second.%s.third.%s.intVal", x0Id, z0Id), 1),
                        hasEntry(format("a.second.%s.third.%s.<order>", x0Id, z0Id), 0),
                        hasEntry(format("a.second.%s.third.%s.<name>", x0Id, z0Id), "Z0")
                ))
        );

        SecondConfiguration x = a.second().get("X");

        // Append new key. It should have <order> = 1.
        x.third().change(xb -> xb.create("Z5", z5 -> {})).get();

        UUID z5Id = ((NamedListNode<?>) a.second().get("X").third().value()).internalId("Z5");

        storageValues = storage.readDataOnRecovery().thenApply(Data::values);

        assertThat(
                storageValues,
                willBe(Matchers.<Map<String, ? extends Serializable>>allOf(
                        aMapWithSize(12),
                        hasEntry(format("a.second.<ids>.X"), x0Id),
                        hasEntry(format("a.second.%s.str", x0Id), "foo"),
                        hasEntry(format("a.second.%s.<order>", x0Id), 0),
                        hasEntry(format("a.second.%s.<name>", x0Id), "X"),
                        hasEntry(format("a.second.%s.third.<ids>.Z0", x0Id), z0Id),
                        hasEntry(format("a.second.%s.third.<ids>.Z5", x0Id), z5Id),
                        hasEntry(format("a.second.%s.third.%s.intVal", x0Id, z0Id), 1),
                        hasEntry(format("a.second.%s.third.%s.<order>", x0Id, z0Id), 0),
                        hasEntry(format("a.second.%s.third.%s.<name>", x0Id, z0Id), "Z0"),
                        hasEntry(format("a.second.%s.third.%s.intVal", x0Id, z5Id), 1),
                        hasEntry(format("a.second.%s.third.%s.<order>", x0Id, z5Id), 1),
                        hasEntry(format("a.second.%s.third.%s.<name>", x0Id, z5Id), "Z5")
                ))
        );

        // Insert new key somewhere in the middle. Index of Z5 should be updated to 2.
        x.third().change(xb -> xb.create(1, "Z2", z2 -> {})).get();

        UUID z2Id = ((NamedListNode<?>) a.second().get("X").third().value()).internalId("Z2");

        storageValues = storage.readDataOnRecovery().thenApply(Data::values);

        assertThat(
                storageValues,
                willBe(Matchers.<Map<String, ? extends Serializable>>allOf(
                        aMapWithSize(16),
                        hasEntry(format("a.second.<ids>.X"), x0Id),
                        hasEntry(format("a.second.%s.str", x0Id), "foo"),
                        hasEntry(format("a.second.%s.<order>", x0Id), 0),
                        hasEntry(format("a.second.%s.<name>", x0Id), "X"),
                        hasEntry(format("a.second.%s.third.<ids>.Z0", x0Id), z0Id),
                        hasEntry(format("a.second.%s.third.<ids>.Z2", x0Id), z2Id),
                        hasEntry(format("a.second.%s.third.<ids>.Z5", x0Id), z5Id),
                        hasEntry(format("a.second.%s.third.%s.intVal", x0Id, z0Id), 1),
                        hasEntry(format("a.second.%s.third.%s.<order>", x0Id, z0Id), 0),
                        hasEntry(format("a.second.%s.third.%s.<name>", x0Id, z0Id), "Z0"),
                        hasEntry(format("a.second.%s.third.%s.intVal", x0Id, z2Id), 1),
                        hasEntry(format("a.second.%s.third.%s.<order>", x0Id, z2Id), 1),
                        hasEntry(format("a.second.%s.third.%s.<name>", x0Id, z2Id), "Z2"),
                        hasEntry(format("a.second.%s.third.%s.intVal", x0Id, z5Id), 1),
                        hasEntry(format("a.second.%s.third.%s.<order>", x0Id, z5Id), 2),
                        hasEntry(format("a.second.%s.third.%s.<name>", x0Id, z5Id), "Z5")
                ))
        );

        // Insert new key somewhere in the middle. Indexes of Z3 and Z5 should be updated to 2 and 3.
        x.third().change(xb -> xb.createAfter("Z2", "Z3", z3 -> {})).get();

        UUID z3Id = ((NamedListNode<?>) a.second().get("X").third().value()).internalId("Z3");

        storageValues = storage.readDataOnRecovery().thenApply(Data::values);

        assertThat(
                storageValues,
                willBe(Matchers.<Map<String, ? extends Serializable>>allOf(
                        aMapWithSize(20),
                        hasEntry(format("a.second.<ids>.X"), x0Id),
                        hasEntry(format("a.second.%s.str", x0Id), "foo"),
                        hasEntry(format("a.second.%s.<order>", x0Id), 0),
                        hasEntry(format("a.second.%s.<name>", x0Id), "X"),
                        hasEntry(format("a.second.%s.third.<ids>.Z0", x0Id), z0Id),
                        hasEntry(format("a.second.%s.third.<ids>.Z2", x0Id), z2Id),
                        hasEntry(format("a.second.%s.third.<ids>.Z3", x0Id), z3Id),
                        hasEntry(format("a.second.%s.third.<ids>.Z5", x0Id), z5Id),
                        hasEntry(format("a.second.%s.third.%s.intVal", x0Id, z0Id), 1),
                        hasEntry(format("a.second.%s.third.%s.<order>", x0Id, z0Id), 0),
                        hasEntry(format("a.second.%s.third.%s.<name>", x0Id, z0Id), "Z0"),
                        hasEntry(format("a.second.%s.third.%s.intVal", x0Id, z2Id), 1),
                        hasEntry(format("a.second.%s.third.%s.<order>", x0Id, z2Id), 1),
                        hasEntry(format("a.second.%s.third.%s.<name>", x0Id, z2Id), "Z2"),
                        hasEntry(format("a.second.%s.third.%s.intVal", x0Id, z3Id), 1),
                        hasEntry(format("a.second.%s.third.%s.<order>", x0Id, z3Id), 2),
                        hasEntry(format("a.second.%s.third.%s.<name>", x0Id, z3Id), "Z3"),
                        hasEntry(format("a.second.%s.third.%s.intVal", x0Id, z5Id), 1),
                        hasEntry(format("a.second.%s.third.%s.<order>", x0Id, z5Id), 3),
                        hasEntry(format("a.second.%s.third.%s.<name>", x0Id, z5Id), "Z5")
                ))
        );

        // Delete keys from the middle. Indexes of Z3 should be updated to 1.
        x.third().change(xb -> xb.delete("Z2").delete("Z5")).get();

        storageValues = storage.readDataOnRecovery().thenApply(Data::values);

        assertThat(
                storageValues,
                willBe(Matchers.<Map<String, ? extends Serializable>>allOf(
                    aMapWithSize(12),
                        hasEntry(format("a.second.<ids>.X"), x0Id),
                        hasEntry(format("a.second.%s.str", x0Id), "foo"),
                        hasEntry(format("a.second.%s.<order>", x0Id), 0),
                        hasEntry(format("a.second.%s.<name>", x0Id), "X"),
                        hasEntry(format("a.second.%s.third.<ids>.Z0", x0Id), z0Id),
                        hasEntry(format("a.second.%s.third.<ids>.Z3", x0Id), z3Id),
                        hasEntry(format("a.second.%s.third.%s.intVal", x0Id, z0Id), 1),
                        hasEntry(format("a.second.%s.third.%s.<order>", x0Id, z0Id), 0),
                        hasEntry(format("a.second.%s.third.%s.<name>", x0Id, z0Id), "Z0"),
                        hasEntry(format("a.second.%s.third.%s.intVal", x0Id, z3Id), 1),
                        hasEntry(format("a.second.%s.third.%s.<order>", x0Id, z3Id), 1),
                        hasEntry(format("a.second.%s.third.%s.<name>", x0Id, z3Id), "Z3")
                ))
        );

        // Delete keys from the middle. Indexes of Z3 should be updated to 1.
        x.third().change(xb -> xb.rename("Z0", "Z1")).get();

        storageValues = storage.readDataOnRecovery().thenApply(Data::values);

        assertThat(
                storageValues,
                willBe(Matchers.<Map<String, ? extends Serializable>>allOf(
                        aMapWithSize(12),
                        hasEntry(format("a.second.<ids>.X"), x0Id),
                        hasEntry(format("a.second.%s.str", x0Id), "foo"),
                        hasEntry(format("a.second.%s.<order>", x0Id), 0),
                        hasEntry(format("a.second.%s.<name>", x0Id), "X"),
                        hasEntry(format("a.second.%s.third.<ids>.Z1", x0Id), z0Id),
                        hasEntry(format("a.second.%s.third.<ids>.Z3", x0Id), z3Id),
                        hasEntry(format("a.second.%s.third.%s.intVal", x0Id, z0Id), 1),
                        hasEntry(format("a.second.%s.third.%s.<order>", x0Id, z0Id), 0),
                        hasEntry(format("a.second.%s.third.%s.<name>", x0Id, z0Id), "Z1"),
                        hasEntry(format("a.second.%s.third.%s.intVal", x0Id, z3Id), 1),
                        hasEntry(format("a.second.%s.third.%s.<order>", x0Id, z3Id), 1),
                        hasEntry(format("a.second.%s.third.%s.<name>", x0Id, z3Id), "Z3")
                ))
        );

        // Delete values on several layers simultaneously. Storage must be empty after that.
        a.second().change(b -> b.delete("X")).get();

        assertThat(storage.readDataOnRecovery().thenApply(Data::values), willBe(anEmptyMap()));
    }

    /** Tests exceptions described in methods signatures. */
    @Test
    public void errors() throws Exception {
        var b = new NamedListNode<>("name", NamedListNodeTest::instantiateChildNode, null);

        b.create("X", x -> {
        }).create("Y", y -> {
        });

        // NPE in keys.
        assertThrows(NullPointerException.class, () -> b.create(null, z -> {
        }));
        assertThrows(NullPointerException.class, () -> b.createOrUpdate(null, z -> {
        }));
        assertThrows(NullPointerException.class, () -> b.create(0, null, z -> {
        }));
        assertThrows(NullPointerException.class, () -> b.rename(null, "Z"));
        assertThrows(NullPointerException.class, () -> b.rename("X", null));
        assertThrows(NullPointerException.class, () -> b.delete(null));

        // NPE in closures.
        assertThrows(NullPointerException.class, () -> b.create("Z", null));
        assertThrows(NullPointerException.class, () -> b.createOrUpdate("Z", null));
        assertThrows(NullPointerException.class, () -> b.create(0, "Z", null));

        // Already existing keys.
        assertThrows(IllegalArgumentException.class, () -> b.create("X", x -> {
        }));
        assertThrows(IllegalArgumentException.class, () -> b.create(0, "X", x -> {
        }));
        assertThrows(IllegalArgumentException.class, () -> b.rename("X", "Y"));

        // Wrong indexes.
        assertThrows(IndexOutOfBoundsException.class, () -> b.create(-1, "Z", z -> {
        }));
        assertThrows(IndexOutOfBoundsException.class, () -> b.create(3, "Z", z -> {
        }));

        // Nonexisting key.
        assertThrows(IllegalArgumentException.class, () -> b.rename("A", "Z"));

        // Operations after delete.
        b.delete("X");
        assertThrows(IllegalArgumentException.class, () -> b.create("X", x -> {
        }));
        assertThrows(IllegalArgumentException.class, () -> b.create(0, "X", x -> {
        }));
        assertThrows(IllegalArgumentException.class, () -> b.rename("X", "Z"));
        assertThrows(IllegalArgumentException.class, () -> b.rename("Y", "X"));

        // Deletion of nonexistent elements doesn't break anything.
        b.delete("X");
        b.delete("Y");
    }

    /**
     * Tests the {@link NamedListNode#update} method.
     */
    @Test
    public void testUpdate() {
        var list = new NamedListNode<SecondChange>("name", NamedListNodeTest::instantiateChildNode, null);

        list.create("foo", ch -> ch.changeStr("bar"));

        assertThat(list.get("foo").str(), is(equalTo("bar")));

        list.update("foo", ch -> ch.changeStr("baz"));

        assertThat(list.get("foo").str(), is(equalTo("baz")));

        list.delete("foo");

        // updating a removed key should throw
        assertThrows(IllegalArgumentException.class, () -> list.update("foo", ch -> {}));
    }

    @Test
    public void testUpdateErrors() {
        var list = new NamedListNode<SecondChange>("name", NamedListNodeTest::instantiateChildNode, null);

        assertThrows(NullPointerException.class, () -> list.update(null, ch -> {}));
        assertThrows(NullPointerException.class, () -> list.update("foo", null));

        // updating a non existent key should throw
        assertThrows(IllegalArgumentException.class, () -> list.update("wrong", ch -> {}));
    }

    @Test
    void testCreateAfterErrors() {
        var list = new NamedListNode<>("name", NamedListNodeTest::instantiateChildNode, null);

        list
                .create("X", x -> {})
                .create("Y", y -> {});

        assertThrows(NullPointerException.class, () -> list.createAfter(null, "Z", z -> {}));

        assertThrows(NullPointerException.class, () -> list.createAfter("X", null, z -> {}));

        assertThrows(NullPointerException.class, () -> list.createAfter("X", "Z", null));

        // inserting an existing key should throw
        assertThrows(IllegalArgumentException.class, () -> list.createAfter("X", "Y", y -> {}));

        // inserting after a missing key should throw
        assertThrows(IllegalArgumentException.class, () -> list.createAfter("A", "Z", z -> {}));

        // inserting after a removed key should throw
        assertThrows(IllegalArgumentException.class, () -> list.delete("X").createAfter("X", "foo", foo -> {}));
    }

    @Test
    public void makeImmutable() {
        var list = new NamedListNode<>("name", NamedListNodeTest::instantiateChildNode, null);

        list.makeImmutable();

        assertThrows(AssertionError.class, () -> list.construct("elem", ConfigurationUtil.EMPTY_CFG_SRC, true));

        assertThrows(AssertionError.class, () -> list.setInternalId("elem", UUID.randomUUID()));

        assertThrows(AssertionError.class, () -> list.reorderKeys(List.of()));

        assertThrows(AssertionError.class, () -> list.create("elem", elem -> {}));
        assertThrows(AssertionError.class, () -> list.create(0, "elem", elem -> {}));
        assertThrows(AssertionError.class, () -> list.createAfter("foo", "elem", elem -> {}));
        assertThrows(AssertionError.class, () -> list.createOrUpdate("elem", elem -> {}));

        assertThrows(AssertionError.class, () -> list.delete("elem"));
        assertThrows(AssertionError.class, () -> list.forceDelete("elem"));

        // Copy is always mutable.
        list.copy().create("elem", elem -> {});
    }

    private static InnerNode instantiateChildNode() {
        return cgen.instantiateNode(SecondConfigurationSchema.class);
    }
}
