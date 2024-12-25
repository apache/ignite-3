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

import java.util.Map;
import java.util.Set;
import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.PublicName;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator;
import org.apache.ignite.internal.configuration.tree.ConstructableTreeNodeTest.ConstantConfigurationSource;
import org.apache.ignite.internal.configuration.tree.InnerNode;
import org.apache.ignite.internal.configuration.tree.polymorphic.PolymorphicTraversableTreeNodeTest.ParentConfigurationSchema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Class for testing configuration construction.
 */
public class PolymorphicConstructableTreeNodeTest {
    private static ConfigurationAsmGenerator cgen;

    @BeforeAll
    public static void beforeAll() {
        cgen = new ConfigurationAsmGenerator();

        cgen.compileRootSchema(
                ParentConfigurationSchema.class,
                Map.of(),
                Map.of(ParentConfigurationSchema.class, Set.of(ParentWithPrimitiveConfigurationSchema.class))
        );
    }

    @AfterAll
    public static void afterAll() {
        cgen = null;
    }

    private static <P extends InnerNode & ParentChange> P newParentInstance() {
        return (P) cgen.instantiateNode(ParentConfigurationSchema.class);
    }

    /**
     * Parent polymorphic instance with primitive properties.
     */
    @PolymorphicConfigInstance(PolymorphicTraversableTreeNodeTest.TYPE_1)
    public static class ParentWithPrimitiveConfigurationSchema extends ParentConfigurationSchema {
        @Value(hasDefault = true)
        @PublicName("has-default")
        public int hasDefault = 100;

        @Value()
        @PublicName("no-default")
        public String noDefault;
    }

    @Test
    public void testConstructHasDefault() {
        var parentNode = newParentInstance();

        ParentWithPrimitiveChange parentWithPrimitive = parentNode.convert(ParentWithPrimitiveChange.class);

        parentNode.construct("has-default", new ConstantConfigurationSource(10), false);

        assertEquals(10, parentWithPrimitive.hasDefault());

        parentNode.constructDefault("has-default");

        assertEquals(100, parentWithPrimitive.hasDefault());
    }

    @Test
    public void testConstructNoDefault() {
        var parentNode = newParentInstance();

        ParentWithPrimitiveChange parentWithPrimitive = parentNode.convert(ParentWithPrimitiveChange.class);

        assertNull(parentWithPrimitive.noDefault());

        parentNode.construct("no-default", new ConstantConfigurationSource("1"), false);

        assertEquals("1", parentWithPrimitive.noDefault());

        parentNode.constructDefault("no-default"); // Does nothing.

        assertEquals("1", parentWithPrimitive.noDefault());
    }
}
