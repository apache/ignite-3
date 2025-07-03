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

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigNode.Attributes;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigNode.Flags;
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
                new ConfigNode(root1, Map.of(ConfigNode.Attributes.NAME, "child"), List.of(), EnumSet.of(Flags.IS_VALUE), Set.of())
        ));

        ConfigNode root2 = createRoot("root1");
        ConfigNode child = new ConfigNode(root2, Map.of(Attributes.NAME, "child"), List.of(), EnumSet.noneOf(Flags.class), Set.of());
        root2.addChildNodes(List.of(child));
        child.addChildNodes(
                new ConfigNode(child, Map.of(Attributes.NAME, "child"), List.of(), EnumSet.of(Flags.IS_VALUE), Set.of())
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
                        EnumSet.of(Flags.IS_VALUE), Set.of())
        ));

        ConfigNode root2 = createRoot("root1");
        root2.addChildNodes(List.of(
                new ConfigNode(
                        root2,
                        Map.of(ConfigNode.Attributes.NAME, "child", Attributes.CLASS, long.class.getCanonicalName()),
                        List.of(),
                        EnumSet.of(Flags.IS_VALUE), Set.of())
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
                        EnumSet.of(Flags.IS_VALUE), Set.of())
        ));

        ConfigNode root2 = createRoot("root1");
        root2.addChildNodes(List.of(
                new ConfigNode(
                        root2,
                        Map.of(ConfigNode.Attributes.NAME, "child"),
                        List.of(new ConfigAnnotation(Deprecated.class.getName())),
                        EnumSet.of(Flags.IS_VALUE), Set.of())
        ));

        // Adding deprecation is compatible change.
        assertCompatible(root1, root2);
        // Removing deprecation is not compatible change.
        assertIncompatible(root2, root1);
    }

    /**
     * Check {@link ConfigurationModule#deletedPrefixes()} functionality.
     */
    @Test
    void testDeleted() {
        // Build previous version config tree.
        ConfigNode root = new ConfigNode(null, Map.of(Attributes.NAME, "root"), List.of(),
                EnumSet.of(Flags.IS_ROOT), Set.of());

        ConfigNode singleProp = new ConfigNode(root, Map.of(Attributes.NAME, "property"), List.of(),
                EnumSet.of(Flags.IS_VALUE), Set.of());

        ConfigNode legacyProp = new ConfigNode(root, Map.of(Attributes.NAME, "legacyProperty"), List.of(),
                EnumSet.of(Flags.IS_VALUE), Set.of());

        ConfigNode compoundProp = new ConfigNode(root, Map.of(Attributes.NAME, "list"), List.of(),
                EnumSet.of(Flags.IS_INTERNAL), Set.of());

        ConfigNode firstMemberOfCompoundProp = new ConfigNode(compoundProp, Map.of(Attributes.NAME, "firstProperty"), List.of(),
                EnumSet.of(Flags.IS_VALUE), Set.of());
        ConfigNode secondMemberOfCompoundProp = new ConfigNode(compoundProp, Map.of(Attributes.NAME, "secondProperty"), List.of(),
                EnumSet.of(Flags.IS_VALUE), Set.of());

        compoundProp.addChildNodes(firstMemberOfCompoundProp, secondMemberOfCompoundProp);

        root.addChildNodes(singleProp, legacyProp, compoundProp);

        List<ConfigNode> snapshotMetadata = List.of(root);

        // Build next version config tree. Some properties are marked as deleted.
        root = new ConfigNode(null, Map.of(Attributes.NAME, "root"), List.of(),
                EnumSet.of(Flags.IS_ROOT), Set.of());

        singleProp = new ConfigNode(root, Map.of(Attributes.NAME, "property"), List.of(),
                EnumSet.of(Flags.IS_VALUE), Set.of());

        compoundProp = new ConfigNode(root, Map.of(Attributes.NAME, "list"), List.of(),
                EnumSet.of(Flags.IS_INTERNAL), Set.of());

        root.addChildNodes(List.of(singleProp, compoundProp));

        List<ConfigNode> currentMetadata = List.of(root);

        ConfigurationModule configModule = new ConfigurationModule() {
            @Override
            public ConfigurationType type() {
                return ConfigurationType.LOCAL;
            }

            @Override
            public Collection<String> deletedPrefixes() {
                return List.of("root.legacyProperty", "root.list.*");
            }
        };

        Set<ConfigurationModule> allModules = Set.of(configModule);

        assertCompatible(snapshotMetadata, currentMetadata, allModules);

        // missed deleted properties
        configModule = new ConfigurationModule() {
            @Override
            public ConfigurationType type() {
                return ConfigurationType.LOCAL;
            }

            @Override
            public Collection<String> deletedPrefixes() {
                return List.of("root.legacyProperty_notExist", "root.list.*");
            }
        };

        allModules = Set.of(configModule);

        assertIncompatible(snapshotMetadata, currentMetadata, allModules);
    }

    @Test
    void testCompatibleRename() {
        ConfigNode oldNode = new ConfigNode(null, Map.of(Attributes.NAME, "oldTestCount"), List.of(),
                EnumSet.of(Flags.IS_VALUE), Set.of());

        ConfigNode newNode = new ConfigNode(null, Map.of(Attributes.NAME, "newTestCount"),
                List.of(),
                EnumSet.of(Flags.IS_VALUE), Set.of("oldTestCount"));

        assertCompatible(oldNode, newNode);

        // Equal configs, still compatible
        oldNode = new ConfigNode(null, Map.of(Attributes.NAME, "newTestCount"),
                List.of(),
                EnumSet.of(Flags.IS_VALUE), Set.of("oldTestCount"));

        newNode = new ConfigNode(null, Map.of(Attributes.NAME, "newTestCount"),
                List.of(),
                EnumSet.of(Flags.IS_VALUE), Set.of("oldTestCount"));

        assertCompatible(oldNode, newNode);

        // TODO: need to have a possibility to check compatibility between outdated (officially no more supported) version and current one.
        // I.e. previous tree with filled deletedPrefixes and current without - need to be compatible too, decided to make it later.
    }

    @Test
    void testInCompatibleRename() {
        ConfigNode oldNode1 = new ConfigNode(null, Map.of(Attributes.NAME, "oldTestCount"), List.of(),
                EnumSet.of(Flags.IS_VALUE), Set.of());

        ConfigNode newNode1 = new ConfigNode(null, Map.of(Attributes.NAME, "newTestCount"),
                List.of(),
                EnumSet.of(Flags.IS_VALUE), Set.of("oldTestCount_missed"));

        assertIncompatible(oldNode1, newNode1);

        // Different root names, not compatible
        ConfigNode root1 = new ConfigNode(null, Map.of(Attributes.NAME, "root1"), List.of(),
                EnumSet.of(Flags.IS_ROOT), Set.of());

        ConfigNode root2 = new ConfigNode(null, Map.of(Attributes.NAME, "root2"), List.of(),
                EnumSet.of(Flags.IS_ROOT), Set.of());

        ConfigNode oldNode2 = new ConfigNode(root1, Map.of(Attributes.NAME, "oldTestCount"), List.of(),
                EnumSet.of(Flags.IS_VALUE), Set.of());

        root1.addChildNodes(oldNode2);

        ConfigNode newNode2 = new ConfigNode(root2, Map.of(Attributes.NAME, "newTestCount"),
                List.of(),
                EnumSet.of(Flags.IS_VALUE), Set.of("oldTestCount"));

        root2.addChildNodes(newNode2);

        assertIncompatible(oldNode1, newNode1);
    }

    /**
     * Test scenario. <br>
     * config ver1 has property : prop1 <br>
     * config ver2 has renamed property : prop1 -> prop2 <br>
     * config ver3 has deleted property : prop1, prop2 <br>
     * <br>
     * Check config transitions are possible: ver1 -> ver2, ver1 -> ver3, ver2 -> ver3
     */
    @Test
    void testDeleteAfterRename() {
        ConfigNode root = new ConfigNode(null, Map.of(Attributes.NAME, "root"), List.of(),
                EnumSet.of(Flags.IS_ROOT), Set.of());

        ConfigNode node1Ver1 = new ConfigNode(root, Map.of(Attributes.NAME, "oldTestCount"), List.of(),
                EnumSet.of(Flags.IS_VALUE), Set.of());

        ConfigNode node2Ver1 = new ConfigNode(root, Map.of(Attributes.NAME, "fixed"), List.of(),
                EnumSet.of(Flags.IS_VALUE), Set.of());

        root.addChildNodes(node1Ver1, node2Ver1);

        List<ConfigNode> metadataVer1 = List.of(root);

        root = new ConfigNode(null, Map.of(Attributes.NAME, "root"), List.of(),
                EnumSet.of(Flags.IS_ROOT), Set.of());

        ConfigNode node1Ver2 = new ConfigNode(root, Map.of(Attributes.NAME, "newTestCount"),
                List.of(),
                EnumSet.of(Flags.IS_VALUE), Set.of("oldTestCount"));

        ConfigNode node2Ver2 = new ConfigNode(root, Map.of(Attributes.NAME, "fixed"), List.of(),
                EnumSet.of(Flags.IS_VALUE), Set.of());

        root.addChildNodes(node1Ver2, node2Ver2);

        List<ConfigNode> metadataVer2 = List.of(root);

        root = new ConfigNode(null, Map.of(Attributes.NAME, "root"), List.of(),
                EnumSet.of(Flags.IS_ROOT), Set.of());

        ConfigNode node2Ver3 = new ConfigNode(root, Map.of(Attributes.NAME, "fixed"), List.of(),
                EnumSet.of(Flags.IS_VALUE), Set.of());

        root.addChildNodes(node2Ver3);

        List<ConfigNode> metadataVer3 = List.of(root);

        ConfigurationModule configModule = new ConfigurationModule() {
            @Override
            public ConfigurationType type() {
                return ConfigurationType.LOCAL;
            }

            @Override
            public Collection<String> deletedPrefixes() {
                return List.of("root.oldTestCount", "root.newTestCount");
            }
        };

        Set<ConfigurationModule> allModules = Set.of(configModule);

        assertCompatible(metadataVer1, metadataVer2);
        assertCompatible(metadataVer1, metadataVer3, allModules);
        assertCompatible(metadataVer2, metadataVer3, allModules);
    }

    private static ConfigNode createRoot(String name) {
        return ConfigNode.createRoot(name, Object.class, ConfigurationType.LOCAL, true);
    }

    private static ConfigNode createChild(ConfigNode root1, String name) {
        return new ConfigNode(
                root1,
                Map.of(ConfigNode.Attributes.NAME, name, Attributes.CLASS, int.class.getCanonicalName()),
                List.of(),
                EnumSet.of(Flags.IS_VALUE), Set.of()
        );
    }

    private static void assertCompatible(ConfigNode oldConfig, ConfigNode newConfig) {
        assertCompatible(List.of(oldConfig), List.of(newConfig));
    }

    private static void assertCompatible(List<ConfigNode> oldConfig, List<ConfigNode> newConfig) {
        assertCompatible(oldConfig, newConfig, Set.of());
    }

    private static void assertCompatible(List<ConfigNode> oldConfig, List<ConfigNode> newConfig, Set<ConfigurationModule> allModules) {
        ConfigurationTreeComparator.ensureCompatible(oldConfig, newConfig, allModules);
    }

    private static void assertIncompatible(ConfigNode oldConfig, ConfigNode newConfig) {
        assertIncompatible(List.of(oldConfig), List.of(newConfig));
    }

    private static void assertIncompatible(List<ConfigNode> oldConfig, List<ConfigNode> newConfig) {
        assertIncompatible(oldConfig, newConfig, Set.of());
    }

    private static void assertIncompatible(List<ConfigNode> oldConfig, List<ConfigNode> newConfig, Set<ConfigurationModule> configModules) {
        try {
            assertCompatible(oldConfig, newConfig, configModules);
        } catch (IllegalStateException ignore) {
            // Expected exception
            return;
        }

        fail("Compatibility check passed unexpectedly.");
    }
}
