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
import java.util.stream.Collectors;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.KeyIgnorer;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigNode.Node;
import org.jetbrains.annotations.Nullable;

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

        compContext.enterInstance(null);
        try {
            compareRoots(snapshotTrees, actualTrees, compContext);
        } finally {
            compContext.leaveInstance();
        }

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
    private static boolean match(ConfigNode candidate, ConfigNode current) {
        // To make debugging easier.
        boolean kindMatches = Objects.equals(current.kind(), candidate.kind());
        boolean nameMatches = matchNames(current, candidate);
        boolean flagMatch = validateFlags(current, candidate);
        boolean deletedPrefixesMatch = current.deletedPrefixes().containsAll(candidate.deletedPrefixes());
        // Value node types can be changed.
        boolean nodeTypeMatches = !candidate.isValue() || Objects.equals(current.type(), candidate.type());
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

    private static boolean matchNames(ConfigNode candidate, ConfigNode current) {
        return Objects.equals(candidate.name(), current.name()) || compareUsingLegacyNames(candidate, current);
    }

    private static boolean compareUsingLegacyNames(ConfigNode candidate, ConfigNode current) {
        return candidate.legacyPropertyNames().contains(current.name());
    }

    private static boolean validateFlags(ConfigNode candidate, ConfigNode current) {
        // If flags are empty then they should always be compatible.
        if (candidate.flags().equals(current.flags())) {
            return true;
        }
        return current.isValue() == candidate.isValue()
                && (!candidate.isInternal() || current.isInternal()) // Public property\tree can't be hidden.
                && current.isNamedNode() == candidate.isNamedNode()
                && current.isInnerNode() == candidate.isInnerNode()
                && (!current.isDeprecated() || candidate.isDeprecated()); // Deprecation shouldn't be removed.
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

        private final ArrayDeque<Optional<String>> instanceTypes = new ArrayDeque<>();

        ComparisonContext(Collection<String> deletedPrefixes) {
            this.deletedItems = KeyIgnorer.fromDeletedPrefixes(deletedPrefixes);
        }

        boolean shouldIgnore(String path) {
            return deletedItems.shouldIgnore(path);
        }

        void reset() {
            errors.clear();
        }

        void enterInstance(@Nullable String instanceType) {
            instanceTypes.addLast(Optional.ofNullable(instanceType));
        }

        void leaveInstance() {
            instanceTypes.pop();
        }

        void addError(Node node, String error) {
            reportError(node.path(), error);
        }

        void addError(ConfigNode node, String error) {
            reportError(node.path(), error);
        }

        private void reportError(String path, String error) {
            Optional<String> opt = instanceTypes.peekLast();
            String instanceType = opt.isPresent() ? opt.get() : null;

            String message;
            if (instanceType == null) {
                message = format("Node: {}: {}", path, error);
            } else {
                message = format("Node: {} [polymorphic-instance-id={}]: {}", path, instanceType, error);
            }

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

    private static void compareRoots(List<ConfigNode> candidateRoots, List<ConfigNode> currentRoots, ComparisonContext context) {
        List<ConfigNode> removed = new ArrayList<>();
        List<ConfigNode> added = new ArrayList<>();
        List<ConfigNode> currentCopy = new ArrayList<>(currentRoots);

        for (ConfigNode root1 : candidateRoots) {
            boolean matchFound = false;

            for (ConfigNode root2 : new ArrayList<>(currentCopy)) {
                if (rootsMatch(root1, root2)) {
                    currentCopy.remove(root2);
                    validate(root1, root2, context);
                    matchFound = true;
                    break;
                }
            }

            if (!matchFound) {
                removed.add(root1);
            }
        }

        added.addAll(currentCopy);

        // Validate new roots
        validateNew(added, context);
        // Reject removed roots.
        validateRemoved(removed, context);
    }

    private static boolean rootsMatch(ConfigNode candidate, ConfigNode current) {
        boolean nameMatches = Objects.equals(candidate.name(), current.name());
        boolean kindMatches = Objects.equals(candidate.kind(), current.kind());
        return nameMatches && kindMatches;
    }

    private static void validate(ConfigNode candidate, ConfigNode current, ComparisonContext context) {
        validate(null, candidate, current, context);
    }

    private static void validate(@Nullable String instanceType, ConfigNode candidate, ConfigNode current, ComparisonContext context) {
        if (!context.errors.isEmpty()) {
            return;
        }
        context.enterInstance(instanceType);
        try {
            compareNodes(candidate, current, context);
        } finally {
            context.leaveInstance();
        }
    }

    private static void validate(Node candidate, Node current, ComparisonContext context) {
        if (!context.errors.isEmpty()) {
            return;
        }

        if (candidate.isPolymorphic() && current.isPolymorphic()) {
            Map<String, ConfigNode> polymorphicCurrent = new HashMap<>(current.nodes());
            Map<String, ConfigNode> polymorphicCandidate = candidate.nodes();

            Map<String, ConfigNode> removed = polymorphicCandidate.entrySet().stream()
                    .filter(e -> !polymorphicCurrent.containsKey(e.getKey()))
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

            validateRemoved(removed.values(), context);
            // new polymorphic instances are not used, so they are always valid. 

            polymorphicCandidate.entrySet().stream()
                    .filter(e -> !removed.containsKey(e.getKey()))
                    .forEach(e -> {
                        ConfigNode instanceCurrent = polymorphicCurrent.get(e.getKey());
                        validate(e.getKey(), e.getValue(), instanceCurrent, context);
                    });
        } else if (candidate.isPolymorphic()) {
            context.addError(candidate,
                    "Can't convert polymorphic node to plain. Because potential reverse change in the future will be incompatible.");
        } else if (current.isPolymorphic()) {
            String polymorphicId = current.defaultPolymorphicInstanceId();

            if (polymorphicId == null) {
                ConfigNode anyNode = current.nodes().values().iterator().next();
                context.addError(anyNode, "New polymorphic node has no default id");
                return;
            }

            // Ensure discriminant field wasn't exits, and we can take default polymorphic_ID
            validate(polymorphicId, candidate.node(), current.nodes().get(polymorphicId), context);
            // additional polymorphic instances are not used, so they are always valid.
        } else {
            compareNodes(candidate.node(), current.node(), context);
        }
    }

    private static void compareNodes(ConfigNode candidate, ConfigNode current, ComparisonContext context) {
        if (!match(candidate, current)) {
            context.addError(candidate, "Node does not match. Previous: " + candidate + ". Current: " + current);
            return;
        }

        validateAnnotations(candidate, current, context);

        compareChildren(candidate.children(), current.children(), context);
    }

    private static void compareChildren(Map<String, Node> candidate, Map<String, Node> current, ComparisonContext context) {
        // Validates matching children.
        // then validates removed and added ones.
        List<Node> removed = new ArrayList<>();
        List<Node> added = new ArrayList<>();
        Map<String, Node> currentCopy = new HashMap<>(current);

        for (Entry<String, Node> candididateEntry : candidate.entrySet()) {
            Node candidateNode = candididateEntry.getValue();
            boolean matchFound = false;

            for (Entry<String, Node> currentEntry : currentCopy.entrySet()) {
                Node nodeB = currentEntry.getValue();
                if (equalNames(candididateEntry, currentEntry)) {
                    matchFound = true;
                    currentCopy.remove(currentEntry.getKey());
                    validate(candidateNode, nodeB, context);
                    break;
                }
            }

            if (!matchFound) {
                removed.add(candidateNode);
            }
        }

        added.addAll(currentCopy.values());

        validateRemovedChildren(removed, context);
        validateNewChildren(added, context);
    }

    private static boolean equalNames(Entry<String, Node> currentEntry, Entry<String, Node> candidateEntry) {
        String candidateName = currentEntry.getKey();
        String currentName = candidateEntry.getKey();
        if (candidateName.equals(currentName)) {
            return true;
        }
        Node candidateNode = candidateEntry.getValue();
        return candidateNode.legacyPropertyNames().stream().anyMatch(n -> n.equals(candidateName));
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
