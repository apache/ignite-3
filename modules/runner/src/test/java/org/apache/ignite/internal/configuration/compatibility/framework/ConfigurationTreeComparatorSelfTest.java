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
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigNode.NodeReference;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigurationTreeComparator.ComparisonContext;
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
                        List.of(),
                        EnumSet.of(Flags.IS_VALUE, Flags.IS_DEPRECATED))
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
    void testDeletedProperty() {
        // Build previous version config tree.
        ConfigNode root = new ConfigNode(null, Map.of(Attributes.NAME, "root"), List.of(),
                EnumSet.of(Flags.IS_ROOT));

        ConfigNode singleProp = new ConfigNode(root, Map.of(Attributes.NAME, "property"), List.of(),
                EnumSet.of(Flags.IS_VALUE));

        ConfigNode legacyProp = new ConfigNode(root, Map.of(Attributes.NAME, "legacyProperty"), List.of(),
                EnumSet.of(Flags.IS_VALUE));

        root.addChildNodes(singleProp, legacyProp);

        List<ConfigNode> snapshotMetadata = List.of(root);

        // Build next version config tree. Some properties are marked as deleted.
        root = new ConfigNode(null, Map.of(Attributes.NAME, "root"), List.of(),
                EnumSet.of(Flags.IS_ROOT));

        singleProp = new ConfigNode(root, Map.of(Attributes.NAME, "property"), List.of(),
                EnumSet.of(Flags.IS_VALUE));

        root.addChildNodes(List.of(singleProp));

        List<ConfigNode> currentMetadata = List.of(root);

        ConfigurationModule configModule = new ConfigurationModule() {
            @Override
            public ConfigurationType type() {
                return ConfigurationType.LOCAL;
            }

            @Override
            public Collection<String> deletedPrefixes() {
                return List.of("root.legacyProperty");
            }
        };

        Set<ConfigurationModule> allModules = Set.of(configModule);

        assertCompatible(snapshotMetadata, currentMetadata, new ComparisonContext(allModules));

        // missed deleted properties
        configModule = new ConfigurationModule() {
            @Override
            public ConfigurationType type() {
                return ConfigurationType.LOCAL;
            }

            @Override
            public Collection<String> deletedPrefixes() {
                return List.of("root.legacyProperty_notExist");
            }
        };

        allModules = Set.of(configModule);

        assertIncompatible(snapshotMetadata, currentMetadata, new ComparisonContext(allModules));
    }

    /**
     * Check {@link ConfigurationModule#deletedPrefixes()} functionality.
     */
    @Test
    void testDeletedSubtree() {
        // Build previous version config tree.
        ConfigNode root = new ConfigNode(null, Map.of(Attributes.NAME, "root"), List.of(),
                EnumSet.of(Flags.IS_ROOT));

        ConfigNode compoundProp = new ConfigNode(root, Map.of(Attributes.NAME, "list"), List.of(),
                EnumSet.of(Flags.IS_INTERNAL));

        ConfigNode firstMemberOfCompoundProp = new ConfigNode(compoundProp, Map.of(Attributes.NAME, "firstProperty"), List.of(),
                EnumSet.of(Flags.IS_VALUE));
        ConfigNode secondMemberOfCompoundProp = new ConfigNode(compoundProp, Map.of(Attributes.NAME, "secondProperty"), List.of(),
                EnumSet.of(Flags.IS_VALUE));

        compoundProp.addChildNodes(firstMemberOfCompoundProp, secondMemberOfCompoundProp);

        root.addChildNodes(compoundProp);

        List<ConfigNode> snapshotMetadata = List.of(root);

        // Build next version config tree. Some properties are marked as deleted.
        root = new ConfigNode(null, Map.of(Attributes.NAME, "root"), List.of(),
                EnumSet.of(Flags.IS_ROOT));

        compoundProp = new ConfigNode(root, Map.of(Attributes.NAME, "list"), List.of(),
                EnumSet.of(Flags.IS_INTERNAL));

        root.addChildNodes(List.of(compoundProp));

        List<ConfigNode> currentMetadata = List.of(root);

        ConfigurationModule configModule = new ConfigurationModule() {
            @Override
            public ConfigurationType type() {
                return ConfigurationType.LOCAL;
            }

            @Override
            public Collection<String> deletedPrefixes() {
                return List.of("root.list.*");
            }
        };

        Set<ConfigurationModule> allModules = Set.of(configModule);

        assertCompatible(snapshotMetadata, currentMetadata, new ComparisonContext(allModules));

        // missed deleted properties
        configModule = new ConfigurationModule() {
            @Override
            public ConfigurationType type() {
                return ConfigurationType.LOCAL;
            }

            @Override
            public Collection<String> deletedPrefixes() {
                return List.of("root.list_notExist.*");
            }
        };

        allModules = Set.of(configModule);

        assertIncompatible(snapshotMetadata, currentMetadata, new ComparisonContext(allModules));
    }

    @Test
    void testDeletedPrefixes() {
        // Build previous version config tree.
        ConfigNode node = new ConfigNode(null, Map.of(Attributes.NAME, "root"), List.of(),
                EnumSet.of(Flags.IS_ROOT), Set.of(), Set.of("prefix1", "prefix2"));

        ConfigNode singleProp = new ConfigNode(node, Map.of(Attributes.NAME, "property"), List.of(),
                EnumSet.of(Flags.IS_VALUE));

        node.addChildNodes(List.of(singleProp));

        List<ConfigNode> snapshotMetadata = List.of(node);

        // Build same deletions config tree.
        ConfigNode nodeWithSameDeletions = new ConfigNode(null, Map.of(Attributes.NAME, "root"), List.of(),
                EnumSet.of(Flags.IS_ROOT), Set.of(), Set.of("prefix1", "prefix2"));

        singleProp = new ConfigNode(nodeWithSameDeletions, Map.of(Attributes.NAME, "property"), List.of(),
                EnumSet.of(Flags.IS_VALUE));

        nodeWithSameDeletions.addChildNodes(List.of(singleProp));

        List<ConfigNode> sameDeletionsMetadata = List.of(nodeWithSameDeletions);

        // Build extend deletions config tree.
        ConfigNode nodeWithExtendDeletions = new ConfigNode(null, Map.of(Attributes.NAME, "root"), List.of(),
                EnumSet.of(Flags.IS_ROOT), Set.of(), Set.of("prefix1", "prefix2", "prefix3"));

        singleProp = new ConfigNode(nodeWithExtendDeletions, Map.of(Attributes.NAME, "property"), List.of(),
                EnumSet.of(Flags.IS_VALUE));

        nodeWithExtendDeletions.addChildNodes(List.of(singleProp));

        List<ConfigNode> extendDeletionsMetadata = List.of(nodeWithExtendDeletions);

        assertCompatible(snapshotMetadata, sameDeletionsMetadata);
        assertCompatible(snapshotMetadata, extendDeletionsMetadata);

        assertIncompatible(extendDeletionsMetadata, snapshotMetadata);
    }

    @Test
    void testCompatibleRename() {
        ConfigNode oldNode = new ConfigNode(null, Map.of(Attributes.NAME, "oldTestCount"), List.of(),
                EnumSet.of(Flags.IS_VALUE));

        ConfigNode newNode = new ConfigNode(null, Map.of(Attributes.NAME, "newTestCount"),
                List.of(),
                EnumSet.of(Flags.IS_VALUE), Set.of("oldTestCount"), List.of());

        assertCompatible(oldNode, newNode);

        // Equal configs, still compatible
        oldNode = new ConfigNode(null, Map.of(Attributes.NAME, "newTestCount"),
                List.of(),
                EnumSet.of(Flags.IS_VALUE), Set.of("oldTestCount"), List.of());

        newNode = new ConfigNode(null, Map.of(Attributes.NAME, "newTestCount"),
                List.of(),
                EnumSet.of(Flags.IS_VALUE), Set.of("oldTestCount"), List.of());

        assertCompatible(oldNode, newNode);

        // TODO: need to have a possibility to check compatibility between outdated (officially no more supported) version and current one.
        // I.e. previous tree with filled deletedPrefixes and current without - need to be compatible too, decided to make it later.
    }

    @Test
    void testInCompatibleRename() {
        ConfigNode oldNode1 = new ConfigNode(null, Map.of(Attributes.NAME, "oldTestCount"), List.of(),
                EnumSet.of(Flags.IS_VALUE));

        ConfigNode newNode1 = new ConfigNode(null, Map.of(Attributes.NAME, "newTestCount"),
                List.of(),
                EnumSet.of(Flags.IS_VALUE), Set.of("oldTestCount_missed"), List.of());

        assertIncompatible(oldNode1, newNode1);

        // Different root names, not compatible
        ConfigNode root1 = new ConfigNode(null, Map.of(Attributes.NAME, "root1"), List.of(),
                EnumSet.of(Flags.IS_ROOT));

        ConfigNode root2 = new ConfigNode(null, Map.of(Attributes.NAME, "root2"), List.of(),
                EnumSet.of(Flags.IS_ROOT));

        ConfigNode oldNode2 = new ConfigNode(root1, Map.of(Attributes.NAME, "oldTestCount"), List.of(),
                EnumSet.of(Flags.IS_VALUE));

        root1.addChildNodes(oldNode2);

        ConfigNode newNode2 = new ConfigNode(root2, Map.of(Attributes.NAME, "newTestCount"),
                List.of(),
                EnumSet.of(Flags.IS_VALUE), Set.of("oldTestCount"), List.of());

        root2.addChildNodes(newNode2);

        assertIncompatible(oldNode1, newNode1);
    }

    /**
     * Test scenario. <br> config ver1 has property : prop1 <br> config ver2 has renamed property : prop1 -> prop2 <br> config ver3 has
     * deleted property : prop1, prop2 <br>
     * <br>
     * Check config transitions are possible: ver1 -> ver2, ver1 -> ver3, ver2 -> ver3
     */
    @Test
    void testDeleteAfterRename() {
        ConfigNode root = new ConfigNode(null, Map.of(Attributes.NAME, "root"), List.of(),
                EnumSet.of(Flags.IS_ROOT));

        ConfigNode node1Ver1 = new ConfigNode(root, Map.of(Attributes.NAME, "oldTestCount"), List.of(),
                EnumSet.of(Flags.IS_VALUE));

        root.addChildNodes(node1Ver1);

        List<ConfigNode> metadataVer1 = List.of(root);

        root = new ConfigNode(null, Map.of(Attributes.NAME, "root"), List.of(),
                EnumSet.of(Flags.IS_ROOT));

        ConfigNode node1Ver2 = new ConfigNode(root, Map.of(Attributes.NAME, "newTestCount"),
                List.of(),
                EnumSet.of(Flags.IS_VALUE), Set.of("oldTestCount"), List.of());

        root.addChildNodes(node1Ver2);

        List<ConfigNode> metadataVer2 = List.of(root);

        root = new ConfigNode(null, Map.of(Attributes.NAME, "root"), List.of(),
                EnumSet.of(Flags.IS_ROOT));

        ConfigNode node = new ConfigNode(root, Map.of(Attributes.NAME, "fixed"), List.of(),
                EnumSet.of(Flags.IS_VALUE));

        root.addChildNodes(node);

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
        assertCompatible(metadataVer1, metadataVer3, new ComparisonContext(allModules));
        assertCompatible(metadataVer2, metadataVer3, new ComparisonContext(allModules));
    }

    @Test
    public void testPolymorphicConfigNoChanges() {
        ConfigNode root = createRoot("root");

        ConfigNode base = createNode(root, "subconfig", "ClassBase");
        base.addChildNodes(createChild(base, "f1"));

        ConfigNode subclassA = createInstanceNode(root, "subconfig", "ClassA", "A");
        subclassA.addChildNodes(createChild(subclassA, "f2"));

        ConfigNode subclassB = createInstanceNode(root, "subconfig", "ClassB", "B");
        subclassB.addChildNodes(createChild(subclassB, "f3"));

        root.addChildReferences(List.of(new NodeReference(List.of(base, subclassA, subclassB))));

        assertCompatible(List.of(root), List.of(root));
    }

    @Test
    public void testPolymorphicConfigAddInstance() {
        ConfigNode root1 = createRoot("root");
        {
            ConfigNode base = createNode(root1, "subconfig", "ClassBase");
            base.addChildNodes(createChild(base, "f1"));

            ConfigNode subclassA = createInstanceNode(root1, "subconfig", "ClassA", "A");
            subclassA.addChildNodes(createChild(subclassA, "f2"));

            root1.addChildReferences(List.of(new NodeReference(List.of(base, subclassA))));
        }

        ConfigNode root2 = createRoot("root");
        {
            ConfigNode base = createNode(root2, "subconfig", "ClassBase");
            base.addChildNodes(createChild(base, "f1"));

            ConfigNode subclassA = createInstanceNode(root2, "subconfig", "ClassA", "A");
            subclassA.addChildNodes(createChild(subclassA, "f2"));

            ConfigNode subclassB = createInstanceNode(root2, "subconfig", "ClassB", "B");
            subclassB.addChildNodes(createChild(subclassB, "f3"));

            root2.addChildReferences(List.of(new NodeReference(List.of(base, subclassA, subclassB))));
        }

        assertCompatible(List.of(root1), List.of(root2));
        // Removing should be an incompatible change.
        assertIncompatible(List.of(root2), List.of(root1));
    }

    @Test
    public void testPolymorphicConfigChangeType() {
        ConfigNode root1 = createRoot("root");
        {
            ConfigNode base = createNode(root1, "subconfig", "ClassBase");
            base.addChildNodes(createChild(base, "f1"));

            ConfigNode subclassA = createInstanceNode(root1, "subconfig", "ClassA", "A");
            subclassA.addChildNodes(createChild(subclassA, "f2"));

            root1.addChildReferences(List.of(new NodeReference(List.of(base, subclassA))));
        }

        ConfigNode root2 = createRoot("root");
        {
            ConfigNode base = createNode(root2, "subconfig", "ClassBase");
            base.addChildNodes(createChild(base, "f1"));

            ConfigNode subclassA = createInstanceNode(root2, "subconfig", "ClassA", "B");
            subclassA.addChildNodes(createChild(subclassA, "f2"));

            root2.addChildReferences(List.of(new NodeReference(List.of(base, subclassA))));
        }

        // Changing instance type is always an incompatible change 
        assertIncompatible(List.of(root1), List.of(root2));
        assertIncompatible(List.of(root2), List.of(root1));
    }

    @Test
    public void testPolymorphicConfigAddInstanceField() {
        ConfigNode root1 = createRoot("root");
        {
            ConfigNode base = createNode(root1, "subconfig", "ClassBase");
            base.addChildNodes(createChild(base, "f1"));

            ConfigNode subclassA = createInstanceNode(root1, "subconfig", "ClassA", "A");
            subclassA.addChildNodes(createChild(subclassA, "f2"));

            root1.addChildReferences(List.of(new NodeReference(List.of(base, subclassA))));
        }

        ConfigNode root2 = createRoot("root");
        {
            ConfigNode base = createNode(root2, "subconfig", "ClassBase");
            base.addChildNodes(createChild(base, "f1"));

            ConfigNode subclassA = createInstanceNode(root2, "subconfig", "ClassA", "A");
            subclassA.addChildNodes(createChild(subclassA, "f2"));
            subclassA.addChildNodes(createChild(subclassA, "f3"));

            root2.addChildReferences(List.of(new NodeReference(List.of(base, subclassA))));
        }

        assertCompatible(List.of(root1), List.of(root2));
        // Removing should be an incompatible change.
        assertIncompatible(List.of(root2), List.of(root1));
    }

    private static ConfigNode createRoot(String name) {
        return ConfigNode.createRoot(name, Object.class, ConfigurationType.LOCAL, true);
    }

    private static ConfigNode createNode(ConfigNode parent, String name, String className) {
        return new ConfigNode(
                parent,
                Map.of(Attributes.NAME, name, Attributes.CLASS, className),
                List.of(),
                EnumSet.noneOf(Flags.class)
        );
    }

    private static ConfigNode createInstanceNode(ConfigNode parent, String name, String className, String instanceType) {
        return new ConfigNode(
                parent,
                Map.of(Attributes.NAME, name, Attributes.CLASS, className, Attributes.INSTANCE_TYPE, instanceType),
                List.of(),
                EnumSet.noneOf(Flags.class)
        );
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
        assertCompatible(oldConfig, newConfig, new ComparisonContext());
    }

    private static void assertCompatible(List<ConfigNode> oldConfig, List<ConfigNode> newConfig, ComparisonContext compContext) {
        ConfigurationTreeComparator.ensureCompatible(oldConfig, newConfig, compContext);
    }

    private static void assertIncompatible(ConfigNode oldConfig, ConfigNode newConfig) {
        assertIncompatible(List.of(oldConfig), List.of(newConfig));
    }

    private static void assertIncompatible(List<ConfigNode> oldConfig, List<ConfigNode> newConfig) {
        assertIncompatible(oldConfig, newConfig, new ComparisonContext());
    }

    private static void assertIncompatible(List<ConfigNode> oldConfig, List<ConfigNode> newConfig, ComparisonContext compContext) {
        try {
            assertCompatible(oldConfig, newConfig, compContext);
        } catch (IllegalStateException ignore) {
            // Expected exception
            return;
        }

        fail("Compatibility check passed unexpectedly.");
    }
}
