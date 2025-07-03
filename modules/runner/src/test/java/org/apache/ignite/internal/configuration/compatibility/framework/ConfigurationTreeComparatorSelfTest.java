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

package org.apache.ignite.internal.configuration.compatibility.framework;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigNode.Attributes;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigNode.Flags;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

/**
 * Self-test for {@link ConfigurationTreeComparator} checks various compatibility scenarios.
 */
public class ConfigurationTreeComparatorSelfTest {

    @Test
    void equalConfigurationCanBeFound() {
        ConfigNode root1 = createRoot("root1");
        root1.addChildNodes(List.of(
                createChild(root1, "child1"),
                createChild(root1, "child2")
        ));

        ConfigNode root2 = createRoot("root1");
        root2.addChildNodes(List.of(
                createChild(root2, "child1"),
                createChild(root2, "child2")
        ));

        ConfigNode anotherRoot = createRoot("root2");
        anotherRoot.addChildNodes(List.of(
                createChild(anotherRoot, "child1"),
                createChild(anotherRoot, "child2")
        ));

        assertCompatible(List.of(root1), List.of(anotherRoot, root2));
    }

    @Test
    void rootCompatibility() {
        {
            ConfigNode root1 = ConfigNode.createRoot("root", Object.class, ConfigurationType.LOCAL, true);
            root1.addChildNodes(createChild(root1, "child"));

            ConfigNode root2 = ConfigNode.createRoot("root", Object.class, ConfigurationType.LOCAL, false);
            root2.addChildNodes(createChild(root2, "child"));

            // Internal root can become public.
            assertCompatible(root1, root2);
            // But not vice versa, public root can't become internal.
            assertIncompatible(root2, root1);
        }
        {
            ConfigNode root1 = ConfigNode.createRoot("root", Object.class, ConfigurationType.LOCAL, true);
            root1.addChildNodes(createChild(root1, "child"));

            ConfigNode root2 = ConfigNode.createRoot("root", Object.class, ConfigurationType.DISTRIBUTED, true);
            root2.addChildNodes(createChild(root2, "child"));

            // Root types can't be changed both ways.
            assertIncompatible(root1, root2);
            assertIncompatible(root2, root1);
        }
    }

    @Test
    void nodeTypeCantBeChanged() {
        ConfigNode root1 = createRoot("root1");
        root1.addChildNodes(List.of(
                new ConfigNode(root1, Map.of(ConfigNode.Attributes.NAME, "child"), List.of(), EnumSet.of(Flags.IS_VALUE))
        ));

        ConfigNode root2 = createRoot("root1");
        ConfigNode child = new ConfigNode(root2, Map.of(Attributes.NAME, "child"), List.of(), EnumSet.noneOf(Flags.class));
        root2.addChildNodes(List.of(child));
        child.addChildNodes(
                new ConfigNode(child, Map.of(Attributes.NAME, "child"), List.of(), EnumSet.of(Flags.IS_VALUE))
        );

        // Leaf node can't become internal node and vice versa.
        assertIncompatible(root1, root2);
        assertIncompatible(root2, root1);
    }

    @Test
    void propertyCantBeRemoved() {
        ConfigNode root1 = createRoot("root1");
        root1.addChildNodes(List.of(
                createChild(root1, "child1")
        ));

        ConfigNode root2 = createRoot("root1");
        root2.addChildNodes(List.of(
                createChild(root2, "child1"),
                createChild(root1, "child2")
        ));

        // Adding a property is compatible change.
        assertCompatible(root1, root2);
        // Removing a property is not compatible change
        assertIncompatible(root2, root1);
    }

    @Test
    void propertyTypeCantBeChanged() {
        ConfigNode root1 = createRoot("root1");
        root1.addChildNodes(List.of(
                new ConfigNode(
                        root1,
                        Map.of(ConfigNode.Attributes.NAME, "child", Attributes.CLASS, int.class.getCanonicalName()),
                        List.of(),
                        EnumSet.of(Flags.IS_VALUE))
        ));

        ConfigNode root2 = createRoot("root1");
        root2.addChildNodes(List.of(
                new ConfigNode(
                        root2,
                        Map.of(ConfigNode.Attributes.NAME, "child", Attributes.CLASS, long.class.getCanonicalName()),
                        List.of(),
                        EnumSet.of(Flags.IS_VALUE))
        ));

        assertIncompatible(root1, root2);
        assertIncompatible(root2, root1);
    }

    @Test
    void deprecatedAnnotationCantBeRemoved() {
        ConfigNode root1 = createRoot("root1");
        root1.addChildNodes(List.of(
                new ConfigNode(
                        root1,
                        Map.of(ConfigNode.Attributes.NAME, "child"),
                        List.of(),
                        EnumSet.of(Flags.IS_VALUE))
        ));

        ConfigNode root2 = createRoot("root1");
        root2.addChildNodes(List.of(
                new ConfigNode(
                        root2,
                        Map.of(ConfigNode.Attributes.NAME, "child"),
                        List.of(new ConfigAnnotation(Deprecated.class.getName())),
                        EnumSet.of(Flags.IS_VALUE))
        ));

        // Adding deprecation is compatible change.
        assertCompatible(root1, root2);
        // Removing deprecation is not compatible change.
        assertIncompatible(root2, root1);
    }

    private static @NotNull ConfigNode createRoot(String name) {
        return ConfigNode.createRoot(name, Object.class, ConfigurationType.LOCAL, true);
    }

    private static ConfigNode createChild(ConfigNode root1, String name) {
        return new ConfigNode(
                root1,
                Map.of(ConfigNode.Attributes.NAME, name, Attributes.CLASS, int.class.getCanonicalName()),
                List.of(),
                EnumSet.of(Flags.IS_VALUE)
        );
    }

    private static void assertCompatible(ConfigNode oldConfig, ConfigNode newConfig) {
        assertCompatible(List.of(oldConfig), List.of(newConfig));
    }

    private static void assertCompatible(List<ConfigNode> oldConfig, List<ConfigNode> newConfig) {
        ConfigurationTreeComparator.ensureCompatible(oldConfig, newConfig);
    }

    private static void assertIncompatible(ConfigNode oldConfig, ConfigNode newConfig) {
        assertIncompatible(List.of(oldConfig), List.of(newConfig));
    }

    private static void assertIncompatible(List<ConfigNode> oldConfig, List<ConfigNode> newConfig) {
        try {
            assertCompatible(oldConfig, newConfig);
        } catch (IllegalStateException ignore) {
            // Expected exception
            return;
        }

        fail("Compatibility check passed unexpectedly.");
    }
}
