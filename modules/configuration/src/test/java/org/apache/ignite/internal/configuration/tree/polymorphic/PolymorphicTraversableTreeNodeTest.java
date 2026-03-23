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

package org.apache.ignite.internal.configuration.tree.polymorphic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.PolymorphicConfig;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PolymorphicId;
import org.apache.ignite.configuration.annotation.PublicName;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator;
import org.apache.ignite.internal.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.NamedListNode;
import org.apache.ignite.internal.configuration.util.ConfigurationUtil;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Class for testing configuration traversal.
 */
public class PolymorphicTraversableTreeNodeTest {
    static final String TYPE_1 = "foo";
    static final String TYPE_2 = "bar";

    private static ConfigurationAsmGenerator cgen;

    @BeforeAll
    public static void beforeAll() {
        cgen = new ConfigurationAsmGenerator();

        cgen.compileRootSchema(
                ParentConfigurationSchema.class,
                Map.of(),
                Map.of(ParentConfigurationSchema.class, Set.of(
                        ParentWithChildConfigurationSchema.class,
                        ParentWithElementsConfigurationSchema.class
                ))
        );
    }

    @AfterAll
    public static void afterAll() {
        cgen = null;
    }

    public static <P extends InnerNode & ParentChange> P newParentInstance() {
        return (P) cgen.instantiateNode(ParentConfigurationSchema.class);
    }

    /**
     * Parent root configuration schema.
     */
    @PolymorphicConfig
    public static class ParentConfigurationSchema {
        @PolymorphicId(hasDefault = true)
        public String type = TYPE_1;
    }

    /**
     * Parent polymorphic instance with a child property.
     */
    @PolymorphicConfigInstance(TYPE_1)
    public static class ParentWithChildConfigurationSchema extends ParentConfigurationSchema {
        @ConfigValue
        @PublicName("my-child")
        public ChildConfigurationSchema child;
    }

    /**
     * Parent polymorphic instance with a named list property.
     */
    @PolymorphicConfigInstance(TYPE_2)
    public static class ParentWithElementsConfigurationSchema extends ParentConfigurationSchema {
        @NamedConfigValue
        @PublicName("my-elements")
        public NamedElementConfigurationSchema elements;
    }

    /**
     * Child configuration schema.
     */
    @Config
    public static class ChildConfigurationSchema {
        @Value
        public int intCfg = 99;
    }

    /**
     * Child named configuration schema.
     */
    @Config
    public static class NamedElementConfigurationSchema {
        @Value
        public String strCfg;
    }

    /**
     * Visit exception.
     */
    private static class VisitException extends RuntimeException {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;
    }

    @Test
    public void traverseChildrenNoTypeDefined() {
        var parentNode = newParentInstance();

        Collection<String> keys = new HashSet<>();

        parentNode.traverseChildren(new ConfigurationVisitor<>() {
            @Override
            public Object visitLeafNode(@Nullable Field field, String key, Serializable val) {
                assertEquals("type", key);
                assertNull(val);

                return keys.add(key);
            }

            @Override
            public Object visitInnerNode(Field field, String key, InnerNode node) {
                throw new VisitException();
            }

            @Override
            public Object visitNamedListNode(Field field, String key, NamedListNode<?> node) {
                throw new VisitException();
            }
        }, true);

        assertEquals(Set.of("type"), keys);
    }

    @Test
    public void traverseChildrenWithTypeDefined() {
        var parentNode = newParentInstance();
        parentNode.convert(ParentWithChildChange.class);

        Collection<String> keys = new HashSet<>();

        parentNode.traverseChildren(new ConfigurationVisitor<>() {
            @Override
            public Object visitLeafNode(@Nullable Field field, String key, Serializable val) {
                return keys.add(key);
            }

            @Override
            public Object visitInnerNode(Field field, String key, InnerNode node) {
                assertEquals("my-child", key);

                return keys.add(key);
            }

            @Override
            public Object visitNamedListNode(Field field, String key, NamedListNode<?> node) {
                throw new VisitException();
            }
        }, true);

        assertEquals(Set.of("my-child", "type"), keys);

        keys.clear();
        parentNode.convert(ParentWithElementsChange.class);

        parentNode.traverseChildren(new ConfigurationVisitor<>() {
            @Override
            public Object visitLeafNode(@Nullable Field field, String key, Serializable val) {
                return keys.add(key);
            }

            @Override
            public Object visitInnerNode(Field field, String key, InnerNode node) {
                throw new VisitException();
            }

            @Override
            public Object visitNamedListNode(Field field, String key, NamedListNode<?> node) {
                assertEquals("my-elements", key);

                return keys.add(key);
            }
        }, true);

        assertEquals(Set.of("my-elements", "type"), keys);
    }

    @Test
    public void traverseSingleChild() {
        var parentNode = newParentInstance();
        parentNode.convert(ParentWithChildChange.class);

        assertThrows(VisitException.class, () ->
                parentNode.traverseChild("type", new ConfigurationVisitor<>() {
                    @Override
                    public Void visitLeafNode(@Nullable Field field, String key, Serializable val) {
                        assertEquals("type", key);

                        throw new VisitException();
                    }
                }, true)
        );

        assertThrows(VisitException.class, () ->
                parentNode.traverseChild("my-child", new ConfigurationVisitor<>() {
                    @Override
                    public @Nullable Void visitInnerNode(@Nullable Field field, String key, InnerNode node) {
                        assertEquals("my-child", key);

                        throw new VisitException();
                    }
                }, true)
        );

        assertThrows(NoSuchElementException.class, () ->
                parentNode.traverseChild("my-elements", ConfigurationUtil.namedListNodeVisitor(), true)
        );

        parentNode.convert(ParentWithElementsChange.class);

        assertThrows(VisitException.class, () ->
                parentNode.traverseChild("my-elements", new ConfigurationVisitor<>() {
                    @Override
                    public @Nullable Void visitNamedListNode(@Nullable Field field, String key, NamedListNode<?> node) {
                        assertEquals("my-elements", key);

                        throw new VisitException();
                    }
                }, true)
        );

        assertThrows(NoSuchElementException.class, () ->
                parentNode.traverseChild("my-child", ConfigurationUtil.innerNodeVisitor(), true)
        );
    }
}
