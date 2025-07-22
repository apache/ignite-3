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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
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
        // Ensure that both collections include the same kinds
        Map<String, List<ConfigNode>> snapshotByKind = new HashMap<>();
        for (ConfigNode node : snapshotTrees) {
            snapshotByKind.computeIfAbsent(node.kind(), (k) -> new ArrayList<>()).add(node);
        }

        Map<String, List<ConfigNode>> actualByKind = new HashMap<>();
        for (ConfigNode node : actualTrees) {
            actualByKind.computeIfAbsent(node.kind(), (k) -> new ArrayList<>()).add(node);
        }

        if (!snapshotByKind.keySet().equals(actualByKind.keySet())) {
            String error = format(
                    "Configuration kind does not match. Expected {} but got {}",
                    snapshotByKind.keySet(),
                    actualByKind.keySet()
            );
            throw new IllegalStateException(error);
        }

        // Validate roots
        for (Map.Entry<String, List<ConfigNode>> e : snapshotByKind.entrySet()) {
            List<ConfigNode> snapshots = e.getValue();
            List<ConfigNode> actuals = actualByKind.get(e.getKey());

            Map<String, ConfigNode> snapshotByName = snapshots.stream()
                    .collect(Collectors.toMap(ConfigNode::name, Function.identity()));

            Map<String, ConfigNode> actualByName = actuals.stream()
                    .collect(Collectors.toMap(ConfigNode::name, Function.identity()));

            Errors errors = new Errors();

            // Validated existing and removed roots
            for (Map.Entry<String, ConfigNode> snapshot : snapshotByName.entrySet()) {
                ConfigNode actual = actualByName.get(snapshot.getKey());

                if (actual == null) {
                    errors.addError("Configuration was removed: " + snapshot.getKey());
                } else {
                    validate(snapshot.getValue(), actual, compContext, errors);
                }
            }

            // Validate new nodes
            for (Map.Entry<String, ConfigNode> actual : actualByName.entrySet()) {
                ConfigNode snapshot = snapshotByName.get(actual.getKey());
                if (snapshot == null) {
                    errors.push(null);
                    try {
                        validateNewChildren(actual.getValue().children().values(), errors);
                    } finally {
                        errors.pop();
                    }
                }
            }

            errors.throwIfNotEmpty();
        }
    }

    /**
     * Checks two nodes for compatibility.
     *
     * @param snapshot Previous version.
     * @param actual Current version.
     * @param compContext Context.
     */
    public static void ensureCompatible(
            ConfigNode snapshot,
            ConfigNode actual,
            ComparisonContext compContext
    ) {
        Errors errors = new Errors();

        validate(snapshot, actual, compContext, errors);

        errors.throwIfNotEmpty();
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

    private static void validateAnnotations(ConfigNode candidate, ConfigNode current, Errors topErrors) {
        List<String> annotationErrors = new ArrayList<>();

        ANNOTATION_VALIDATOR.validate(candidate, current, annotationErrors);

        for (String error : annotationErrors) {
            topErrors.addError(candidate, error);
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

        ComparisonContext(Collection<String> deletedPrefixes) {
            this.deletedItems = KeyIgnorer.fromDeletedPrefixes(deletedPrefixes);
        }

        boolean shouldIgnore(String path) {
            return deletedItems.shouldIgnore(path);
        }
    }

    private static void validate(ConfigNode candidate, ConfigNode current, ComparisonContext context, Errors errors) {
        validate(null, candidate, current, context, errors);
    }

    private static void validate(@Nullable String instanceType, ConfigNode a, ConfigNode b, ComparisonContext context, Errors errors) {
        if (!errors.errors.isEmpty()) {
            return;
        }
        errors.push(instanceType);
        try {
            compareNodes(a, b, context, errors);
        } finally {
            errors.pop();
        }
    }

    private static void validate(Node a, Node b, ComparisonContext context, Errors errors) {
        if (a.isPolymorphic() && b.isPolymorphic()) {
            if (!Objects.equals(a.defaultId(), b.defaultId())) {
                ConfigNode anyNode = a.nodes().values().iterator().next();
                String message = format(
                        "Default polymorphic instance id does not match. Expected: {} got {}", a.defaultId(), b.defaultId()
                );
                errors.addError(anyNode, message);
                return;
            }

            Map<String, ConfigNode> polymorphicB = new HashMap<>(b.nodes());
            Map<String, ConfigNode> polymorphicA = a.nodes();

            Map<String, ConfigNode> removed = polymorphicA.entrySet().stream()
                    .filter(e -> !polymorphicB.containsKey(e.getKey()))
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

            validateRemoved(removed.values(), context, errors);
            // new polymorphic instances are not used, so they are always valid. 

            polymorphicA.entrySet().stream()
                    .filter(e -> !removed.containsKey(e.getKey()))
                    .forEach(e -> {
                        ConfigNode b1 = polymorphicB.get(e.getKey());
                        validate(e.getKey(), e.getValue(), b1, context, errors);
                    });
        } else if (a.isPolymorphic()) {
            errors.addError(a,
                    "Can't convert polymorphic node to plain. Because potential reverse change in the future will be incompatible.");
        } else if (b.isPolymorphic()) {
            String polymorphicId = b.defaultId();

            if (polymorphicId == null) {
                ConfigNode anyNode = b.nodes().values().iterator().next();
                errors.addError(anyNode, "New polymorphic node has no default id");
                return;
            }

            // Ensure discriminant field wasn't exits, and we can take default polymorphic_ID
            validate(polymorphicId, a.node(), b.nodes().get(polymorphicId), context, errors);
            // additional polymorphic instances are not used, so they are always valid.
        } else {
            compareNodes(a.node(), b.node(), context, errors);
        }
    }

    private static void compareNodes(ConfigNode a, ConfigNode b, ComparisonContext context, Errors errors) {
        if (!match(a, b)) {
            errors.addError(a, "Node does not match. Previous: " + a + ". Current: " + b);
            return;
        }

        validateAnnotations(a, b, errors);

        compareChildren(a.children(), b.children(), context, errors);
    }

    private static void compareChildren(Map<String, Node> a, Map<String, Node> b, ComparisonContext context, Errors errors) {
        // Validates matching children.
        // Then validates removed and added ones.
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
                    validate(nodeA, nodeB, context, errors);
                    break;
                }
            }

            if (!matchFound) {
                removed.add(nodeA);
            }
        }

        added.addAll(copyB.values());

        validateRemovedChildren(removed, context, errors);
        validateNewChildren(added, errors);
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

    private static void validateNew(Collection<ConfigNode> nodes, Errors errors) {
        for (ConfigNode node : nodes) {
            if (!node.hasDefault()) {
                errors.addError(node, "Added a node with no default value");
            }
        }
    }

    private static void validateRemoved(Collection<ConfigNode> nodes, ComparisonContext context, Errors errors) {
        for (ConfigNode node : nodes) {
            if (!context.shouldIgnore(node.path())) {
                errors.addError(node, "Node was removed");
            }
        }
    }

    private static void validateNewChildren(Collection<Node> nodes, Errors errors) {
        for (Node node : nodes) {
            if (!node.hasDefault()) {
                errors.addError(node, "Added a node with no default value");
            }
        }
    }

    private static void validateRemovedChildren(Collection<Node> nodes, ComparisonContext context, Errors errors) {
        for (Node node : nodes) {
            if (!context.shouldIgnore(node.path())) {
                errors.addError(node, "Node was removed");
            }
        }
    }

    static class Errors {

        private final List<String> errors = new ArrayList<>();

        private final ArrayDeque<Optional<String>> instanceTypes = new ArrayDeque<>();

        void push(@Nullable String instanceType) {
            instanceTypes.addLast(Optional.ofNullable(instanceType));
        }

        void pop() {
            instanceTypes.pop();
        }

        void addError(String error) {
            errors.add(error);
        }

        void addError(Node node, String error) {
            reportError(node.path(), error);
        }

        void addError(ConfigNode node, String error) {
            reportError(node.path(), error);
        }

        private void reportError(String path, String error) {
            Optional<String> opt = instanceTypes.peekLast();
            ;
            String instanceType = opt.isPresent() ? opt.get() : null;

            String message;
            if (instanceType == null) {
                message = format("Node: {}: {}", path, error);
            } else if (instanceType.isEmpty()) {
                message = format("Node: {} [polymorphic-base]: {}", path, error);
            } else {
                message = format("Node: {} [polymorphicId={}]: {}", path, instanceType, error);
            }

            throw new IllegalStateException(message);
        }

        private void throwIfNotEmpty() {
            if (!errors.isEmpty()) {
                StringBuilder message = new StringBuilder("There are incompatible changes:").append(System.lineSeparator());
                for (String error : errors) {
                    message.append('\t').append(error).append(System.lineSeparator());
                }

                errors.add(message.toString());
            }
        }
    }
}
