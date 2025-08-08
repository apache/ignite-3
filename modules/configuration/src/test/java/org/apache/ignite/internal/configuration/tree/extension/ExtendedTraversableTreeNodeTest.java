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

package org.apache.ignite.internal.configuration.tree.extension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.ConfigValue;
import org.apache.ignite.configuration.annotation.ConfigurationExtension;
import org.apache.ignite.configuration.annotation.NamedConfigValue;
import org.apache.ignite.configuration.annotation.PublicName;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator;
import org.apache.ignite.internal.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.NamedListNode;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Class for testing configuration traversal.
 */
public class ExtendedTraversableTreeNodeTest {
    private static ConfigurationAsmGenerator cgen;

    @BeforeAll
    public static void beforeAll() {
        cgen = new ConfigurationAsmGenerator();

        cgen.compileRootSchema(
                ParentConfigurationSchema.class,
                Map.of(ParentConfigurationSchema.class, Set.of(
                        ParentWithChildConfigurationSchema.class,
                        ParentWithElementsConfigurationSchema.class
                )),
                Map.of()
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
    @Config
    public static class ParentConfigurationSchema {
        @Value
        @PublicName("my-str")
        public String str;
    }

    /**
     * Parent extension with a child property.
     */
    @ConfigurationExtension
    public static class ParentWithChildConfigurationSchema extends ParentConfigurationSchema {
        @ConfigValue
        @PublicName("my-child")
        public ChildConfigurationSchema child;
    }

    /**
     * Parent extension with a named list property.
     */
    @ConfigurationExtension
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
        public int intCfg;
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

    /**
     * Test for "traverseChildren" method implementation on generated inner nodes classes.
     */
    @Test
    public void traverseChildren() {
        var parentNode = newParentInstance();

        Collection<String> keys = new HashSet<>();

        parentNode.traverseChildren(new ConfigurationVisitor<>() {
            @Override
            public Object visitLeafNode(@Nullable Field field, String key, Serializable val) {
                assertEquals("my-str", key);

                return keys.add(key);
            }

            @Override
            public Object visitInnerNode(Field field, String key, InnerNode node) {
                assertEquals("my-child", key);

                return keys.add(key);
            }

            @Override
            public Object visitNamedListNode(Field field, String key, NamedListNode<?> node) {
                assertEquals("my-elements", key);

                return keys.add(key);
            }
        }, true);

        assertEquals(Set.of("my-child", "my-elements", "my-str"), keys);
    }

    @Test
    public void traverseChild() {
        var parentNode = newParentInstance();

        assertThrows(VisitException.class, () ->
                parentNode.traverseChild("my-str", new ConfigurationVisitor<>() {
                    @Override
                    public Void visitLeafNode(@Nullable Field field, String key, Serializable val) {
                        assertEquals("my-str", key);

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

        assertThrows(VisitException.class, () ->
                parentNode.traverseChild("my-elements", new ConfigurationVisitor<>() {
                    @Override
                    public @Nullable Void visitNamedListNode(@Nullable Field field, String key, NamedListNode<?> node) {
                        assertEquals("my-elements", key);

                        throw new VisitException();
                    }
                }, true)
        );
    }
}
