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

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.KeyIgnorer;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigNode.Node;

/**
 * Compares two configuration trees (snapshot and current).
 */
public class ConfigurationTreeComparator {

    private static final ConfigAnnotationsValidator ANNOTATION_VALIDATOR = new ConfigAnnotationsValidator();

    /**
     * Validates the current configuration is compatible with the snapshot.
     */
    public static void ensureCompatible(
            List<ConfigNode> snapshotTrees,
            List<ConfigNode> actualTrees,
            ComparisonContext compContext
    ) {
        compContext.reset();

        compareRoots(snapshotTrees, actualTrees, compContext);

        compContext.throwIfNotEmpty();
    }

    /**
     * Compares the configuration trees are equals by dumping their state to string.
     */
    public static void compare(List<ConfigNode> tree1, List<ConfigNode> tree2) {
        String dump1 = dumpTree(tree1);
        String dump2 = dumpTree(tree2);
        assertEquals(dump1, dump2, "Configuration metadata mismatch");
    }

    /**
     * Returns {@code true} if given node is compatible with candidate node, {@code false} otherwise.
     */
    private static boolean match(ConfigNode node, ConfigNode candidate) {
        // To make debugging easier.
        boolean kindMatches = Objects.equals(candidate.kind(), node.kind());
        boolean nameMatches = matchNames(candidate, node);
        boolean flagMatch = validateFlags(candidate, node);
        boolean deletedPrefixesMatch = candidate.deletedPrefixes().containsAll(node.deletedPrefixes());
        // Value node types can be changed.
        boolean nodeTypeMatches = !node.isValue() || Objects.equals(candidate.type(), node.type());
        return kindMatches
                && nameMatches
                && flagMatch
                && deletedPrefixesMatch
                && nodeTypeMatches;
    }

    private static void validateAnnotations(ConfigNode candidate, ConfigNode current, ComparisonContext context) {
        List<String> annotationErrors = new ArrayList<>();

        ANNOTATION_VALIDATOR.validate(candidate, current, annotationErrors);

        for (String error : annotationErrors) {
            context.addError(candidate, error);
        }
    }

    private static boolean matchNames(ConfigNode candidate, ConfigNode node) {
        return Objects.equals(candidate.name(), node.name()) || compareUsingLegacyNames(candidate, node);
    }

    private static boolean compareUsingLegacyNames(ConfigNode candidate, ConfigNode node) {
        return candidate.legacyPropertyNames().contains(node.name());
    }

    private static boolean validateFlags(ConfigNode candidate, ConfigNode node) {
        // If flags are empty then they should always be compatible.
        if (candidate.flags().equals(node.flags())) {
            return true;
        }
        return node.isValue() == candidate.isValue()
                && (!candidate.isInternal() || node.isInternal()) // Public property\tree can't be hidden.
                && node.isNamedNode() == candidate.isNamedNode()
                && node.isInnerNode() == candidate.isInnerNode()
                && (!node.isDeprecated() || candidate.isDeprecated()); // Deprecation shouldn't be removed.
    }

    private static String dumpTree(List<ConfigNode> nodes) {
        DumpingShuttle shuttle = new DumpingShuttle();
        nodes.forEach(n -> n.accept(shuttle));
        return shuttle.toString();
    }

    /**
     * Configuration tree visitor that dumps tree state to string.
     */
    private static class DumpingShuttle implements ConfigShuttle {
        private final StringBuilder sb = new StringBuilder();

        @Override
        public void visit(ConfigNode node) {
            sb.append(node.digest()).append('\n');
        }

        @Override
        public String toString() {
            return sb.toString();
        }
    }

    /** Holder class for comparison context. */
    public static class ComparisonContext {
        /** Creates context from current configuration. */
        public static ComparisonContext create(Set<ConfigurationModule> configurationModules) {
            Set<String> prefixes = configurationModules.stream()
                    .map(ConfigurationModule::deletedPrefixes)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet());

            return new ComparisonContext(prefixes);
        }

        private final KeyIgnorer deletedItems;

        private final List<String> errors = new ArrayList<>();

        ComparisonContext(Collection<String> deletedPrefixes) {
            this.deletedItems = KeyIgnorer.fromDeletedPrefixes(deletedPrefixes);
        }

        boolean shouldIgnore(String path) {
            return deletedItems.shouldIgnore(path);
        }

        void reset() {
            errors.clear();
        }

        void addError(Node node, String error) {
            reportError(node.path(), error);
        }

        void addError(ConfigNode node, String error) {
            reportError(node.path(), error);
        }

        private void reportError(String path, String error) {
            String message = format("Node: {}: {}", path, error);
            errors.add(message);
        }

        private void throwIfNotEmpty() {
            if (errors.isEmpty()) {
                return;
            }

            StringBuilder message = new StringBuilder("There are incompatible changes:").append(System.lineSeparator());
            for (String error : errors) {
                message.append('\t').append(error).append(System.lineSeparator());
            }

            throw new IllegalStateException(message.toString());
        }
    }


    private static void compareRoots(List<ConfigNode> roots1, List<ConfigNode> roots2, ComparisonContext context) {
        List<ConfigNode> removed = new ArrayList<>();
        List<ConfigNode> added = new ArrayList<>();
        List<ConfigNode> copy2 = new ArrayList<>(roots2);

        for (ConfigNode root1 : roots1) {
            boolean matchFound = false;

            for (ConfigNode root2 : new ArrayList<>(copy2)) {
                if (rootsMatch(root1, root2)) {
                    copy2.remove(root2);
                    validate(root1, root2, context);
                    matchFound = true;
                    break;
                }
            }

            if (!matchFound) {
                removed.add(root1);
            }
        }

        added.addAll(copy2);

        // Validate new roots
        validateNew(added, context);
        // Reject removed roots.
        validateRemoved(removed, context);
    }

    private static boolean rootsMatch(ConfigNode a, ConfigNode b) {
        boolean nameMatches = Objects.equals(a.name(), b.name());
        boolean kindMatches = Objects.equals(a.kind(), b.kind());
        return nameMatches && kindMatches;
    }

    private static void validate(Node a, Node b, ComparisonContext context) {
        compareNodes(a.node(), b.node(), context);
    }

    private static void validate(ConfigNode candidate, ConfigNode current, ComparisonContext context) {
        if (!context.errors.isEmpty()) {
            return;
        }
        compareNodes(candidate, current, context);
    }

    private static void compareNodes(ConfigNode a, ConfigNode b, ComparisonContext context) {
        if (!match(a, b)) {
            context.addError(a, "Node does not match. Previous: " + a + ". Current: " + b);
            return;
        }

        validateAnnotations(a, b, context);

        compareChildren(a.children(), b.children(), context);
    }

    private static void compareChildren(Map<String, Node> a, Map<String, Node> b, ComparisonContext context) {
        // Validates matching children.
        // then validates removed and added ones.
        List<Node> removed = new ArrayList<>();
        List<Node> added = new ArrayList<>();
        Map<String, Node> copyB = new HashMap<>(b);

        for (Entry<String, Node> entryA : a.entrySet()) {
            Node nodeA = entryA.getValue();
            boolean matchFound = false;

            for (Entry<String, Node> entryB : copyB.entrySet()) {
                Node nodeB = entryB.getValue();
                if (equalNames(entryA, entryB)) {
                    matchFound = true;
                    copyB.remove(entryB.getKey());
                    validate(nodeA, nodeB, context);
                    break;
                }
            }

            if (!matchFound) {
                removed.add(nodeA);
            }
        }

        added.addAll(copyB.values());

        validateRemovedChildren(removed, context);
        validateNewChildren(added, context);
    }

    private static boolean equalNames(Entry<String, Node> entryA, Entry<String, Node> entryB) {
        String nameA = entryA.getKey();
        String nameB = entryB.getKey();
        if (nameA.equals(nameB)) {
            return true;
        }
        Node nodeB = entryB.getValue();
        return nodeB.legacyPropertyNames().stream().anyMatch(n -> n.equals(nameA));
    }

    private static void validateNew(Collection<ConfigNode> nodes, ComparisonContext context) {
        // Validate nodes recursively, adding a new node is valid if all its value nodes have default. 
        for (ConfigNode node : nodes) {
            if (node.isValue() && !node.hasDefault()) {
                context.addError(node, "Added a node with no default value");
            }
            validateNewChildren(node.children().values(), context);
        }
    }

    private static void validateRemoved(Collection<ConfigNode> nodes, ComparisonContext context) {
        for (ConfigNode node : nodes) {
            if (!context.shouldIgnore(node.path())) {
                context.addError(node, "Node was removed");
            }
        }
    }

    private static void validateNewChildren(Collection<Node> nodes, ComparisonContext context) {
        // Validate nodes recursively, adding a new node is valid if all its value nodes have default.
        for (Node node : nodes) {
            if (node.isValue() && !node.hasDefault()) {
                context.addError(node, "Added a value with no default");
            } else {
                validateNew(List.of(node.node()), context);
            }
        }
    }

    private static void validateRemovedChildren(Collection<Node> nodes, ComparisonContext context) {
        for (Node node : nodes) {
            if (!context.shouldIgnore(node.path())) {
                context.addError(node, "Node was removed");
            }
        }
    }
}
