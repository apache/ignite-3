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
import java.util.stream.Stream;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigNode.Attributes;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigNode.Flags;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigurationTreeComparator.ComparisonContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Self-test for {@link ConfigurationTreeComparator} checks various compatibility scenarios.
 */
public class ConfigurationTreeComparatorSelfTest {

    @Test
    void equalConfigurationCanBeFound() {
        ConfigNode root1 = createRoot("root1");
        root1.addChildNodes(List.of(
                createChild("child1"),
                createChild("child2")
        ));

        ConfigNode root2 = createRoot("root1");
        root2.addChildNodes(List.of(
                createChild("child1"),
                createChild("child2")
        ));

        ConfigNode anotherRoot = createRoot("root2");
        anotherRoot.addChildNodes(List.of(
                createChild("child1"),
                createChild("child2")
        ));

        assertCompatible(List.of(root1), List.of(anotherRoot, root2));
    }

    @Test
    void rootCompatibility() {
        {
            ConfigNode root1 = ConfigNode.createRoot("root", Object.class, ConfigurationType.LOCAL, true);
            root1.addChildNodes(createChild("child"));

            ConfigNode root2 = ConfigNode.createRoot("root", Object.class, ConfigurationType.LOCAL, false);
            root2.addChildNodes(createChild("child"));

            // Internal root can become public.
            assertCompatible(root1, root2);
            // But not vice versa, public root can't become internal.
            assertIncompatible(root2, root1);
        }
        {
            ConfigNode root1 = ConfigNode.createRoot("root", Object.class, ConfigurationType.LOCAL, true);
            root1.addChildNodes(createChild("child"));

            ConfigNode root2 = ConfigNode.createRoot("root", Object.class, ConfigurationType.DISTRIBUTED, true);
            root2.addChildNodes(createChild("child"));

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
        ConfigNode child = new ConfigNode(root2, Map.of(Attributes.NAME, "child"), List.of(), EnumSet.of(Flags.IS_INNER_NODE));
        root2.addChildNodes(List.of(child));
        child.addChildNodes(
                new ConfigNode(child, Map.of(Attributes.NAME, "child"), List.of(), EnumSet.of(Flags.IS_VALUE))
        );

        // Leaf node can't become internal node and vice versa.
        assertIncompatible(root1, root2);
        assertIncompatible(root2, root1);
    }

    @Test
    void innerNodeTypeCantBeChanged() {
        ConfigNode root1 = createRoot("root1");
        ConfigNode root2 = createRoot("root1");

        ConfigNode inner1 = new ConfigNode(root1, Map.of(Attributes.NAME, "inner"), List.of(), EnumSet.of(Flags.IS_NAMED_NODE));
        ConfigNode inner2 = new ConfigNode(root2, Map.of(Attributes.NAME, "inner"), List.of(), EnumSet.of(Flags.IS_INNER_NODE));

        ConfigNode leafNode1 = new ConfigNode(inner1, Map.of(Attributes.NAME, "child"), List.of(), EnumSet.of(Flags.IS_VALUE));
        ConfigNode leafNode2 = new ConfigNode(inner2, Map.of(Attributes.NAME, "child"), List.of(), EnumSet.of(Flags.IS_VALUE));

        root1.addChildNodes(inner1);
        root2.addChildNodes(inner2);
        inner1.addChildNodes(leafNode1);
        inner2.addChildNodes(leafNode2);

        assertIncompatible(root1, root2);
        assertIncompatible(root2, root1);
    }

    @Test
    void propertyCantBeRemoved() {
        ConfigNode root1 = createRoot("root1");
        root1.addChildNodes(List.of(
                createChild("child1")
        ));

        ConfigNode root2 = createRoot("root1");
        root2.addChildNodes(List.of(
                createChild("child1"),
                createChild("child2")
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
                        EnumSet.of(Flags.IS_VALUE, Flags.HAS_DEFAULT))
        ));

        ConfigNode root2 = createRoot("root1");
        root2.addChildNodes(List.of(
                new ConfigNode(
                        root2,
                        Map.of(ConfigNode.Attributes.NAME, "child"),
                        List.of(),
                        EnumSet.of(Flags.IS_VALUE, Flags.IS_DEPRECATED, Flags.HAS_DEFAULT))
        ));

        // Adding deprecation is compatible change.
        assertCompatible(root1, root2);
        // Removing deprecation is not compatible change.
        assertIncompatible(root2, root1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void addFieldToConfiguration(boolean hasDefault) {
        Set<Flags> newFieldFlags = createFlags(hasDefault);

        ConfigNode root1 = createRoot("root1");
        root1.addChildNodes(List.of(
                createChild("child1"),
                createChild("child2")
        ));

        ConfigNode root2 = createRoot("root1");
        root2.addChildNodes(List.of(
                createChild("child1"),
                createChild("child2"),
                createChild("child3", newFieldFlags)
        ));

        if (hasDefault) {
            assertCompatible(List.of(root1), List.of(root2));
            assertIncompatible(List.of(root2), List.of(root1));
        } else {
            assertIncompatible(List.of(root1), List.of(root2));
            assertIncompatible(List.of(root2), List.of(root1));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void addNewRoot(boolean hasDefault) {
        Set<Flags> fieldFlags = createFlags(hasDefault);

        ConfigNode root1 = createRoot("root1");
        {
            root1.addChildNodes(List.of(
                    createChild("child1"),
                    createChild("child2")
            ));
        }

        ConfigNode root2 = createRoot("root2");
        {
            ConfigNode node = createChild("node1", Set.of());
            node.addChildNodes(List.of(createChild("f", fieldFlags)));

            root2.addChildNodes(List.of(
                    createChild("child1"),
                    createChild("child2"),
                    node
            ));
        }

        if (hasDefault) {
            assertCompatible(List.of(root1), List.of(root1, root2));
            assertIncompatible(List.of(root2, root1), List.of(root1));
        } else {
            assertIncompatible(List.of(root1), List.of(root1, root2));
            assertIncompatible(List.of(root2, root1), List.of(root1));
        }
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

        assertCompatible(snapshotMetadata, currentMetadata, ComparisonContext.create(allModules));

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

        assertIncompatible(snapshotMetadata, currentMetadata, ComparisonContext.create(allModules));
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
                EnumSet.of(Flags.IS_INNER_NODE));

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
                EnumSet.of(Flags.IS_INNER_NODE));

        root.addChildNodes(List.of(compoundProp));

        List<ConfigNode> currentMetadata = List.of(root);

        assertCompatible(snapshotMetadata, currentMetadata, new ComparisonContext(List.of("root.list.*")));
        // missed deleted properties
        assertIncompatible(snapshotMetadata, currentMetadata, new ComparisonContext(List.of("root.list_notExist.*")));
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
        ConfigNode root1 = createRoot("root");

        ConfigNode oldNode = new ConfigNode(null, Map.of(Attributes.NAME, "oldTestCount"), List.of(),
                EnumSet.of(Flags.IS_VALUE));
        root1.addChildNodes(oldNode);

        ConfigNode root2 = createRoot("root");

        ConfigNode newNode = new ConfigNode(null, Map.of(Attributes.NAME, "newTestCount"),
                List.of(),
                EnumSet.of(Flags.IS_VALUE), Set.of("oldTestCount"), List.of());
        root2.addChildNodes(newNode);

        assertCompatible(root1, root2);

        root1 = createRoot("root");
        // Equal configs, still compatible
        oldNode = new ConfigNode(null, Map.of(Attributes.NAME, "newTestCount"),
                List.of(),
                EnumSet.of(Flags.IS_VALUE), Set.of("oldTestCount"), List.of());
        root1.addChildNodes(oldNode);

        root2 = createRoot("root");

        newNode = new ConfigNode(null, Map.of(Attributes.NAME, "newTestCount"),
                List.of(),
                EnumSet.of(Flags.IS_VALUE), Set.of("oldTestCount"), List.of());
        root2.addChildNodes(newNode);

        assertCompatible(root1, root2);

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

        // Incorrectly renaming leaf nodes.

        root1 = createRoot("root1");
        oldNode2 = createChild("oldTestCount");
        root1.addChildNodes(oldNode2);

        root2 = createRoot("root1");
        newNode2 = createChild("newTestCount", Set.of(Flags.IS_VALUE, Flags.HAS_DEFAULT), Set.of("oldTestCount_misspelled"), List.of());
        root2.addChildNodes(newNode2);

        assertIncompatible(root1, root2);

        // Incorrectly renaming intermediate nodes.

        root1 = createRoot("root1");
        oldNode2 = createChild("node1", Set.of());
        oldNode2.addChildNodes(createChild("value"));
        root1.addChildNodes(oldNode2);

        root2 = createRoot("root1");
        newNode2 = createChild("node_new", Set.of(), Set.of("node_misspelled"), List.of());
        newNode2.addChildNodes(createChild("value"));
        root2.addChildNodes(newNode2);

        assertIncompatible(root1, root2);
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
                EnumSet.of(Flags.IS_ROOT));

        ConfigNode node1Ver1 = new ConfigNode(root, Map.of(Attributes.NAME, "oldTestCount"), List.of(),
                EnumSet.of(Flags.IS_VALUE, Flags.HAS_DEFAULT));

        root.addChildNodes(node1Ver1);

        List<ConfigNode> metadataVer1 = List.of(root);

        root = new ConfigNode(null, Map.of(Attributes.NAME, "root"), List.of(),
                EnumSet.of(Flags.IS_ROOT));

        ConfigNode node1Ver2 = new ConfigNode(root, Map.of(Attributes.NAME, "newTestCount"),
                List.of(),
                EnumSet.of(Flags.IS_VALUE, Flags.HAS_DEFAULT), Set.of("oldTestCount"), List.of());

        root.addChildNodes(node1Ver2);

        List<ConfigNode> metadataVer2 = List.of(root);

        root = new ConfigNode(null, Map.of(Attributes.NAME, "root"), List.of(),
                EnumSet.of(Flags.IS_ROOT));

        ConfigNode node = new ConfigNode(root, Map.of(Attributes.NAME, "fixed"), List.of(),
                EnumSet.of(Flags.IS_VALUE, Flags.HAS_DEFAULT));

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
        assertCompatible(metadataVer1, metadataVer3, ComparisonContext.create(allModules));
        assertCompatible(metadataVer2, metadataVer3, ComparisonContext.create(allModules));
    }

    @Test
    public void polymorphicConfigNoChanges() {
        ConfigNode root = createRoot("root");

        ConfigNode subclassA = createInstanceNode("config", "A");
        subclassA.addChildNodes(createChild("f1"));
        subclassA.addChildNodes(createChild("f2"));

        ConfigNode subclassB = createInstanceNode("config", "B");
        subclassB.addChildNodes(createChild("f1"));
        subclassB.addChildNodes(createChild("f3"));

        root.addPolymorphicNode("config", Map.of("A", subclassA, "B", subclassB), "A");

        assertCompatible(List.of(root), List.of(root));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void polymorphicConfigAddInstance(boolean hasDefault) {
        Set<Flags> newFieldFlags = createFlags(hasDefault);

        ConfigNode root1 = createRoot("root");
        {
            ConfigNode subclassA = createInstanceNode("config", "A");
            subclassA.addChildNodes(createChild("f1"));

            root1.addPolymorphicNode("config", Map.of("A", subclassA), "A");
        }

        ConfigNode root2 = createRoot("root");
        {
            ConfigNode subclassA = createInstanceNode("config", "A");
            subclassA.addChildNodes(createChild("f1"));

            ConfigNode subclassB = createInstanceNode("config", "B");
            subclassB.addChildNodes(createChild("f1"));
            subclassB.addChildNodes(createChild("f2", newFieldFlags));

            root2.addPolymorphicNode("config", Map.of("A", subclassA, "B", subclassB), "A");
        }

        assertCompatible(List.of(root1), List.of(root2));
        // Removing should be an incompatible change.
        assertIncompatible(List.of(root2), List.of(root1));
    }

    @Test
    public void polymorphicCanChangeDefaultId() {
        ConfigNode root1 = createRoot("root");
        {
            ConfigNode subclassA = createInstanceNode("config", "A");
            subclassA.addChildNodes(createChild("t"));
            subclassA.addChildNodes(createChild("f1"));

            ConfigNode subclassB = createInstanceNode("config", "B");
            subclassB.addChildNodes(createChild("t"));
            subclassB.addChildNodes(createChild("f2"));

            root1.addPolymorphicNode("config", Map.of("A", subclassA, "B", subclassB), "A");
        }

        ConfigNode root2 = createRoot("root");
        {
            ConfigNode subclassA = createInstanceNode("config", "A");
            subclassA.addChildNodes(createChild("t"));
            subclassA.addChildNodes(createChild("f1"));

            ConfigNode subclassB = createInstanceNode("config", "B");
            subclassB.addChildNodes(createChild("t"));
            subclassB.addChildNodes(createChild("f2"));

            root2.addPolymorphicNode("config", Map.of("A", subclassA, "B", subclassB), "B");
        }

        assertCompatible(List.of(root1), List.of(root2));
        assertCompatible(List.of(root2), List.of(root1));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void polymorphicConfigAddInstanceField(boolean hasDefault) {
        Set<Flags> newFieldFlags = createFlags(hasDefault);

        ConfigNode root1 = createRoot("root");
        {
            ConfigNode subclassA = createInstanceNode("config", "A");
            subclassA.addChildNodes(createChild("t"));
            subclassA.addChildNodes(createChild("f1"));
            subclassA.addChildNodes(createChild("f2"));

            root1.addPolymorphicNode("config", Map.of("A", subclassA), "A");
        }

        ConfigNode root2 = createRoot("root");
        {
            ConfigNode subclassA = createInstanceNode("config", "A");
            subclassA.addChildNodes(createChild("t"));
            subclassA.addChildNodes(createChild("f1"));
            subclassA.addChildNodes(createChild("f2"));
            subclassA.addChildNodes(createChild("f3", newFieldFlags));

            root2.addPolymorphicNode("config", Map.of("A", subclassA), "A");
        }

        if (hasDefault) {
            assertCompatible(List.of(root1), List.of(root2));
        } else {
            assertIncompatible(List.of(root1), List.of(root2));
        }

        // Removing should be an incompatible change.
        assertIncompatible(List.of(root2), List.of(root1));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void polymorphicConfigExtract2Subclasses(boolean hasDefault) {
        Set<Flags> fieldFlags = createFlags(hasDefault);

        ConfigNode root1 = createRoot("root");
        {
            ConfigNode base = createChild("config");
            base.addChildNodes(createChild("t"));
            base.addChildNodes(createChild("f1"));
            base.addChildNodes(createChild("f2"));
            base.addChildNodes(createChild("f3", fieldFlags));

            root1.addChildNodes(List.of(base));
        }

        ConfigNode root2 = createRoot("root");
        {
            ConfigNode subclass1 = createInstanceNode("config", "A");
            subclass1.addChildNodes(createChild("t"));
            subclass1.addChildNodes(createChild("f1"));
            subclass1.addChildNodes(createChild("f2"));
            subclass1.addChildNodes(createChild("f3", fieldFlags));

            ConfigNode subclass2 = createInstanceNode("config", "B");
            subclass2.addChildNodes(createChild("t"));
            subclass2.addChildNodes(createChild("f1"));
            subclass2.addChildNodes(createChild("f3", fieldFlags));

            root2.addPolymorphicNode("config", Map.of("A", subclass1, "B", subclass2), "A");
        }

        // Converting to polymorphic
        assertCompatible(List.of(root1), List.of(root2));
        // Polymorphic to non-polymorphic is incompatible
        assertIncompatible(List.of(root2), List.of(root1));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void polymorphicConfigExtractInstanceAddNewField(boolean hasDefault) {
        Set<Flags> newFieldFlags = createFlags(hasDefault);

        ConfigNode root1 = createRoot("root");
        {
            ConfigNode base = createChild("config");
            base.addChildNodes(createChild("t"));
            base.addChildNodes(createChild("f1"));
            base.addChildNodes(createChild("f2"));
            base.addChildNodes(createChild("f3"));

            root1.addChildNodes(List.of(base));
        }

        ConfigNode root2 = createRoot("root");
        {
            ConfigNode subclass = createInstanceNode("config", "A");
            subclass.addChildNodes(createChild("t"));
            subclass.addChildNodes(createChild("f1"));
            subclass.addChildNodes(createChild("f2"));
            subclass.addChildNodes(createChild("f3"));
            subclass.addChildNodes(createChild("f4", newFieldFlags));
            subclass.addChildNodes(createChild("f5", newFieldFlags));

            root2.addPolymorphicNode("config", Map.of("A", subclass), "A");
        }

        // Extracting a subclass + adding a new field
        if (hasDefault) {
            assertCompatible(root1, root2);
        } else {
            assertIncompatible(root1, root2);
        }
        // A new field is removed - this change is not compatible
        assertIncompatible(root2, root1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void polymorphicConfigExtractWithNestedConfig(boolean hasDefault) {
        Set<Flags> newFieldFlags = createFlags(hasDefault);

        ConfigNode root1 = createRoot("root");
        {
            ConfigNode base1 = createChild("config1");
            base1.addChildNodes(createChild("t"));
            base1.addChildNodes(createChild("f1"));
            base1.addChildNodes(createChild("f2"));
            base1.addChildNodes(createChild("f3"));

            ConfigNode base2 = createChild("config2");
            base2.addChildNodes(createChild("y"));
            base2.addChildNodes(createChild("x1"));
            base2.addChildNodes(createChild("x2"));

            base1.addChildNodes(List.of(base2));

            root1.addChildNodes(List.of(base1));
        }

        ConfigNode root2 = createRoot("root");
        {
            ConfigNode subclass1 = createInstanceNode("config1", "A");
            subclass1.addChildNodes(createChild("t"));
            subclass1.addChildNodes(createChild("f1"));
            subclass1.addChildNodes(createChild("f2"));
            subclass1.addChildNodes(createChild("f3"));
            subclass1.addChildNodes(createChild("f4", newFieldFlags));
            subclass1.addChildNodes(createChild("f5", newFieldFlags));

            ConfigNode subclass2 = createInstanceNode("config2", "C");
            subclass2.addChildNodes(createChild("y"));
            subclass2.addChildNodes(createChild("x1"));
            subclass2.addChildNodes(createChild("x2"));
            subclass2.addChildNodes(createChild("x3", newFieldFlags));
            subclass2.addChildNodes(createChild("x4", newFieldFlags));

            subclass1.addPolymorphicNode("config2", Map.of("C", subclass2), "C");

            root2.addPolymorphicNode("config1", Map.of("A", subclass1), "A");
        }

        // Extracting a subclass + adding a new field
        if (hasDefault) {
            assertCompatible(root1, root2);
        } else {
            assertIncompatible(root1, root2);
        }
        // A new field is removed - this change is not compatible
        assertIncompatible(root2, root1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void polymorphicConfigMoveFieldToSubclasses(boolean hasDefault) {
        Set<Flags> fieldFlags = createFlags(hasDefault);

        ConfigNode root1 = createRoot("root");
        {
            ConfigNode subclass1 = createInstanceNode("config", "A");
            subclass1.addChildNodes(createChild("t"));
            subclass1.addChildNodes(createChild("f1"));
            subclass1.addChildNodes(createChild("f2", fieldFlags));
            subclass1.addChildNodes(createChild("f3"));

            ConfigNode subclass2 = createInstanceNode("config", "B");
            subclass2.addChildNodes(createChild("t"));
            subclass2.addChildNodes(createChild("f1"));
            subclass2.addChildNodes(createChild("f4"));

            root1.addPolymorphicNode("config", Map.of("A", subclass1, "B", subclass2), "A");
        }

        ConfigNode root2 = createRoot("root");
        {
            ConfigNode subclass1 = createInstanceNode("config", "A");
            subclass1.addChildNodes(createChild("t"));
            subclass1.addChildNodes(createChild("f1"));
            subclass1.addChildNodes(createChild("f2", fieldFlags));
            subclass1.addChildNodes(createChild("f3"));

            ConfigNode subclass2 = createInstanceNode("config", "B");
            subclass2.addChildNodes(createChild("t"));
            subclass2.addChildNodes(createChild("f1"));
            subclass2.addChildNodes(createChild("f2", fieldFlags));
            subclass2.addChildNodes(createChild("f4"));

            root2.addPolymorphicNode("config", Map.of("A", subclass1, "B", subclass2), "A");
        }

        // Moving a field from a subclass to the base class
        if (hasDefault) {
            assertCompatible(root1, root2);
            assertIncompatible(root2, root1);
        } else {
            assertIncompatible(root1, root2);
            assertIncompatible(root2, root1);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void polymorphicConfigCannotMoveFromOneInstanceToAnother(boolean hasDefault) {
        Set<Flags> fieldFlags = createFlags(hasDefault);

        ConfigNode root1 = createRoot("root");
        {
            ConfigNode subclass1 = createInstanceNode("config", "A");
            subclass1.addChildNodes(createChild("t"));
            subclass1.addChildNodes(createChild("f1"));
            subclass1.addChildNodes(createChild("f2"));
            subclass1.addChildNodes(createChild("f3", fieldFlags));

            ConfigNode subclass2 = createInstanceNode("config", "B");
            subclass2.addChildNodes(createChild("t"));
            subclass2.addChildNodes(createChild("f1"));
            subclass2.addChildNodes(createChild("f4"));

            root1.addPolymorphicNode("config", Map.of("A", subclass1, "B", subclass2), "A");
        }

        ConfigNode root2 = createRoot("root");
        {
            ConfigNode subclass1 = createInstanceNode("config", "A");
            subclass1.addChildNodes(createChild("t"));
            subclass1.addChildNodes(createChild("f1"));
            subclass1.addChildNodes(createChild("f2"));

            ConfigNode subclass2 = createInstanceNode("config", "B");
            subclass2.addChildNodes(createChild("t"));
            subclass2.addChildNodes(createChild("f1"));
            subclass2.addChildNodes(createChild("f3", fieldFlags));
            subclass2.addChildNodes(createChild("f4"));

            root2.addPolymorphicNode("config", Map.of("A", subclass1, "B", subclass2), "A");
        }

        assertIncompatible(root1, root2);
        assertIncompatible(root2, root1);
    }

    private static Stream<Arguments> defaultDefault() {
        return Stream.of(
                Arguments.of(true, true),
                Arguments.of(false, true),
                Arguments.of(true, false),
                Arguments.of(false, false)
        );
    }

    @ParameterizedTest
    @MethodSource("defaultDefault")
    public void polymorphicConfigMoveToSubclassFieldDefChange(boolean hasDefault, boolean hasDefaultForResult) {
        Set<Flags> fieldFlags1 = createFlags(hasDefault);
        Set<Flags> fieldFlags2 = createFlags(hasDefaultForResult);

        ConfigNode root1 = createRoot("root");
        {
            ConfigNode subclass1 = createInstanceNode("config", "A");
            subclass1.addChildNodes(createChild("t"));
            subclass1.addChildNodes(createChild("f1"));
            subclass1.addChildNodes(createChild("f2"));
            subclass1.addChildNodes(createChild("f3"));

            ConfigNode subclass2 = createInstanceNode("config", "B");
            subclass2.addChildNodes(createChild("t"));
            subclass2.addChildNodes(createChild("f1"));
            subclass2.addChildNodes(createChild("f4", fieldFlags1));

            root1.addPolymorphicNode("config", Map.of("A", subclass1, "B", subclass2), "A");
        }

        ConfigNode root2 = createRoot("root");
        {
            ConfigNode subclass1 = createInstanceNode("config", "A");
            subclass1.addChildNodes(createChild("t"));
            subclass1.addChildNodes(createChild("f1"));
            subclass1.addChildNodes(createChild("f2"));
            subclass1.addChildNodes(createChild("f3"));
            subclass1.addChildNodes(createChild("f4", fieldFlags2));

            ConfigNode subclass2 = createInstanceNode("config", "B");
            subclass2.addChildNodes(createChild("t"));
            subclass2.addChildNodes(createChild("f1"));
            subclass2.addChildNodes(createChild("f2"));
            subclass2.addChildNodes(createChild("f4", fieldFlags2));

            root2.addPolymorphicNode("config", Map.of("A", subclass1, "B", subclass2), "A");
        }

        if (hasDefaultForResult) {
            assertCompatible(root1, root2);
            assertIncompatible(root2, root1);
        } else {
            assertIncompatible(root1, root2);
            assertIncompatible(root2, root1);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void polymorphicConfigMoveFieldTo2SubclassesNested(boolean hasDefault) {
        Set<Flags> fieldFlags = createFlags(hasDefault);

        ConfigNode root1 = createRoot("root");
        {
            ConfigNode init = createChild("init");
            init.addChildNodes(createChild("i"));
            root1.addChildNodes(List.of(init));

            ConfigNode subclass1 = createInstanceNode("config", "A");
            subclass1.addChildNodes(createChild("t"));
            subclass1.addChildNodes(createChild("f1"));
            subclass1.addChildNodes(createChild("f2", fieldFlags));
            subclass1.addChildNodes(createChild("f3"));

            ConfigNode subclass2 = createInstanceNode("config", "B");
            subclass2.addChildNodes(createChild("t"));
            subclass2.addChildNodes(createChild("f1"));
            subclass2.addChildNodes(createChild("f4"));

            init.addPolymorphicNode("config", Map.of("A", subclass1, "B", subclass2), "A");
        }

        ConfigNode root2 = createRoot("root");
        {
            ConfigNode init = createChild("init");
            init.addChildNodes(createChild("i"));
            root2.addChildNodes(List.of(init));

            ConfigNode subclass1 = createInstanceNode("config", "A");
            subclass1.addChildNodes(createChild("t"));
            subclass1.addChildNodes(createChild("f1"));
            subclass1.addChildNodes(createChild("f2", fieldFlags));
            subclass1.addChildNodes(createChild("f3"));

            ConfigNode subclass2 = createInstanceNode("config", "B");
            subclass2.addChildNodes(createChild("t"));
            subclass2.addChildNodes(createChild("f1"));
            subclass2.addChildNodes(createChild("f2", fieldFlags));
            subclass2.addChildNodes(createChild("f4"));

            init.addPolymorphicNode("config", Map.of("A", subclass1, "B", subclass2), "A");
        }

        if (hasDefault) {
            assertCompatible(root1, root2);
            assertIncompatible(root2, root1);
        } else {
            assertIncompatible(root1, root2);
            assertIncompatible(root2, root1);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void polymorphicConfigExtractSubclassesAddNewFieldNestedSubclass(boolean hasDefault) {
        Set<Flags> newFieldFlags = createFlags(hasDefault);

        ConfigNode root1 = createRoot("root");
        {
            ConfigNode init = createChild("init");
            init.addChildNodes(createChild("t_i"));

            ConfigNode subinit = createInstanceNode("init", "I");
            subinit.addChildNodes(createChild("t_i"));
            subinit.addChildNodes(createChild("i"));

            root1.addChildNodes(List.of(subinit));

            ConfigNode subclass1 = createInstanceNode("config", "A");
            subclass1.addChildNodes(createChild("t"));
            subclass1.addChildNodes(createChild("f1"));
            subclass1.addChildNodes(createChild("f2"));
            subclass1.addChildNodes(createChild("f3"));

            ConfigNode subclass2 = createInstanceNode("config", "B");
            subclass2.addChildNodes(createChild("t"));
            subclass2.addChildNodes(createChild("f1"));
            subclass2.addChildNodes(createChild("f4"));

            subinit.addPolymorphicNode("config", Map.of("A", subclass1, "B", subclass2), "A");
        }

        ConfigNode root2 = createRoot("root");
        {
            ConfigNode init = createChild("init");
            init.addChildNodes(createChild("t_i"));
            init.addChildNodes(createChild("j", newFieldFlags));

            ConfigNode subinit = createInstanceNode("init", "I");
            subinit.addChildNodes(createChild("t_i"));
            subinit.addChildNodes(createChild("i"));
            subinit.addChildNodes(createChild("j", newFieldFlags));

            root2.addChildNodes(List.of(subinit));

            ConfigNode subclass1 = createInstanceNode("config", "C");
            subclass1.addChildNodes(createChild("t"));
            subclass1.addChildNodes(createChild("f1"));
            subclass1.addChildNodes(createChild("f2"));
            subclass1.addChildNodes(createChild("f3"));

            ConfigNode subclass2 = createInstanceNode("config", "E");
            subclass2.addChildNodes(createChild("t"));
            subclass2.addChildNodes(createChild("f1"));
            subclass2.addChildNodes(createChild("f2"));
            subclass2.addChildNodes(createChild("f4"));

            subinit.addPolymorphicNode("config", Map.of("A", subclass1, "B", subclass2), "A");
        }

        if (hasDefault) {
            assertCompatible(root1, root2);
            assertIncompatible(root2, root1);
        } else {
            assertIncompatible(root1, root2);
            assertIncompatible(root2, root1);
        }
    }

    private static ConfigNode createRoot(String name) {
        return ConfigNode.createRoot(name, Object.class, ConfigurationType.LOCAL, true);
    }

    private static ConfigNode createInstanceNode(String name, String instanceId) {
        return new ConfigNode(
                null,
                Map.of(Attributes.NAME, name, Attributes.CLASS, "Object", Attributes.POLYMORPHIC_INSTANCE_ID, instanceId),
                List.of(),
                EnumSet.noneOf(Flags.class)
        );
    }

    private static ConfigNode createChild(String name) {
        return createChild(name, Set.of(), Set.of(), List.of());
    }

    private static ConfigNode createChild(String name, Set<Flags> flags) {
        return createChild(name, flags, Set.of(), List.of());
    }

    private static ConfigNode createChild(String name, Set<Flags> flags, Set<String> legacyNames, List<String> deletedPrefixes) {
        return new ConfigNode(
                null,
                Map.of(ConfigNode.Attributes.NAME, name, Attributes.CLASS, "Object"),
                List.of(),
                flags.isEmpty() ? EnumSet.noneOf(Flags.class) : EnumSet.copyOf(flags),
                legacyNames,
                deletedPrefixes
        );
    }

    private static void assertCompatible(ConfigNode oldConfig, ConfigNode newConfig) {
        assertCompatible(List.of(oldConfig), List.of(newConfig));
    }

    private static void assertCompatible(List<ConfigNode> oldConfig, List<ConfigNode> newConfig) {
        assertCompatible(oldConfig, newConfig, new ComparisonContext(List.of()));
    }

    private static void assertCompatible(List<ConfigNode> oldConfig, List<ConfigNode> newConfig, ComparisonContext compContext) {
        ConfigurationTreeComparator.ensureCompatible(oldConfig, newConfig, compContext);
    }

    private static void assertIncompatible(ConfigNode oldConfig, ConfigNode newConfig) {
        assertIncompatible(List.of(oldConfig), List.of(newConfig));
    }

    private static void assertIncompatible(List<ConfigNode> oldConfig, List<ConfigNode> newConfig) {
        assertIncompatible(oldConfig, newConfig, new ComparisonContext(List.of()));
    }

    private static void assertIncompatible(List<ConfigNode> oldConfig, List<ConfigNode> newConfig, ComparisonContext compContext) {
        try {
            ConfigurationTreeComparator.ensureCompatible(oldConfig, newConfig, compContext);
        } catch (IllegalStateException e) {
            // Expected exception
            System.err.println("Error: " + e.getMessage());
            return;
        }

        fail("Compatibility check passed unexpectedly.");
    }

    private static Set<Flags> createFlags(boolean hasDefault) {
        return hasDefault ? Set.of(Flags.HAS_DEFAULT, Flags.IS_VALUE) : Set.of(Flags.IS_VALUE);
    }
}
