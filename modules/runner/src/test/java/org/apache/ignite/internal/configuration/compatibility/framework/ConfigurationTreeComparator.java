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
import java.util.HashSet;
import java.util.LinkedHashMap;
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
import org.apache.ignite.internal.util.CollectionUtils;

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
                    validateConfigNode(null, snapshot.getValue(), actual, compContext, errors);
                }
            }

            // Validate new nodes
            for (Map.Entry<String, ConfigNode> actual : actualByName.entrySet()) {
                ConfigNode snapshot = snapshotByName.get(actual.getKey());
                if (snapshot == null) {
                    validateNewConfigNode(null, actual.getValue(), compContext, errors);
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
        validateConfigNode(null, snapshot, actual, compContext, errors);

        if (!errors.errors.isEmpty()) {
            StringBuilder message = new StringBuilder("There are incompatible changes:").append(System.lineSeparator());
            for (String error : errors.errors) {
                message.append('\t').append(error).append(System.lineSeparator());
            }

            throw new IllegalStateException(message.toString());
        }
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

    private static void validateAnnotations(ConfigNode original, ConfigNode updated, Errors topErrors) {
        List<String> annotationErrors = new ArrayList<>();

        ANNOTATION_VALIDATOR.validate(original, updated, annotationErrors);

        for (String error : annotationErrors) {
            topErrors.addError(original, error);
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

        private final Set<String> skipAddRemoveKeys = new HashSet<>();

        ComparisonContext(Collection<String> deletedPrefixes) {
            this.deletedItems = KeyIgnorer.fromDeletedPrefixes(deletedPrefixes);
        }

        boolean shouldIgnore(String path) {
            return deletedItems.shouldIgnore(path);
        }

        boolean shouldIgnore(ConfigNode node, String childName) {
            return shouldIgnore(node.path() + "." + childName);
        }

        boolean ignoreAddOrRemove(String path) {
            return skipAddRemoveKeys.contains(path);
        }

        boolean ignoreAddOrRemove(ConfigNode node, String childName) {
            return ignoreAddOrRemove(node.path() + "." + childName);
        }
    }

    private static void validateConfigNode(
            @Nullable String instanceType,
            ConfigNode original,
            ConfigNode updated,
            ComparisonContext context,
            Errors errors
    ) {
        errors.push(instanceType);
        doValidateConfigNode(original, updated, context, errors);
        errors.pop();
    }

    private static void validateNewConfigNode(
            @Nullable String instanceType,
            ConfigNode updated,
            ComparisonContext compContext,
            Errors errors
    ) {
        if (compContext.shouldIgnore(updated.path())) {
            return;
        }
        errors.push(instanceType);

        for (Entry<String, Node> e : updated.children().entrySet()) {
            Node childNode = e.getValue();
            if (childNode.isValue() && !childNode.hasDefault()) {
                errors.addChildError(updated, e.getKey(), "Added a node with no default value");
            }

            if (childNode.isPolymorphic()) {
                for (Map.Entry<String, ConfigNode> p : childNode.nodes().entrySet()) {
                    ConfigNode node = p.getValue();
                    validateNewConfigNode(p.getKey(), node, compContext, errors);
                }
            } else {
                ConfigNode node = childNode.node();
                validateNewConfigNode(null, node, compContext, errors);
            }
        }

        errors.pop();
    }

    private static void doValidateConfigNode(ConfigNode original, ConfigNode updated, ComparisonContext context, Errors errors) {
        if (context.shouldIgnore(original.path())) {
            return;
        }

        if (!match(original, updated)) {
            errors.addError(original, "Node does not match. Previous: " + original + ". Current: " + updated);
            return;
        }

        validateAnnotations(original, updated, errors);

        Set<String> originalChildrenNames = original.children().keySet();
        Set<String> updatedChildrenNames = updated.children().keySet();

        // Check for removed nodes
        for (String childName : originalChildrenNames) {
            if (updatedChildrenNames.contains(childName)) {
                continue;
            }

            String path = original.path() + "." + childName;
            if (context.shouldIgnore(path)) {
                continue;
            }

            if (updatedChildrenNames.stream().noneMatch(name -> {
                Node node = updated.children().get(name);
                return node != null && node.legacyPropertyNames().contains(childName);
            })) {
                if (context.ignoreAddOrRemove(original, childName)) {
                    continue;
                }
                errors.addChildError(original, childName, "Node was removed");
            }
        }

        validateChildren(original, updated, context, errors);
    }

    private static void validateChildren(
            ConfigNode original,
            ConfigNode updated,
            ComparisonContext context,
            Errors errors
    ) {
        for (Entry<String, Node> e : original.children().entrySet()) {
            String childName = e.getKey();
            // Removed noded have already been handled
            if (!updated.children().containsKey(childName)) {
                continue;
            }

            Node originalChild = e.getValue();
            Node updatedChild = updated.children().get(childName);

            validateChildNode(original, childName, originalChild, updatedChild, context, errors);
        }

        Set<String> originalChildrenNames = original.children().keySet();

        // Validate new nodes
        for (Entry<String, Node> e : updated.children().entrySet()) {
            String childName = e.getKey();
            Node childNode = updated.children().get(childName);

            if (context.shouldIgnore(original, childName)) {
                continue;
            }

            // This child exists in original node under this name or under one of its legacy names.
            boolean existingChildNode = originalChildrenNames.contains(childName)
                    || childNode.legacyPropertyNames().stream().anyMatch(originalChildrenNames::contains);

            if (existingChildNode) {
                continue;
            }

            if (!childNode.hasDefault() && childNode.isValue() && !context.ignoreAddOrRemove(original, childName)) {
                errors.addChildError(original, childName, "Added a node with no default value");
            }
        }
    }

    private static void validateChildNode(
            ConfigNode original,
            String childName,
            Node originalChild,
            Node updatedChild,
            ComparisonContext context,
            Errors errors
    ) {
        if (!originalChild.isPolymorphic() && !updatedChild.isPolymorphic()) {
            // Both are single nodes, recursively check
            validateConfigNode(null, originalChild.node(), updatedChild.node(), context, errors);
        } else if (originalChild.isPolymorphic() != updatedChild.isPolymorphic()) {
            // Check if node type changed (single vs polymorphic)

            // If changing from single to polymorphic, check if it's a legal conversion
            if (!originalChild.isPolymorphic() && updatedChild.isPolymorphic()) {
                validateSingleToPolymorphic(originalChild, updatedChild, context, errors);
            } else {
                validatePolymorphicToSingle(original, errors, context, childName, originalChild, updatedChild);
            }
        } else {
            assert originalChild.isPolymorphic() && updatedChild.isPolymorphic() : "Expected poly vs poly";

            validatePolymorphicToPolymorphic(original, context, errors, childName, originalChild, updatedChild);
        }
    }

    private static void validateSingleToPolymorphic(
            Node originalChild,
            Node updatedChild,
            ComparisonContext context,
            Errors errors
    ) {
        ConfigNode singleNode = originalChild.node();
        Map<String, ConfigNode> polymorphicNodes = updatedChild.nodes();

        // Transaction from a single node to polymorphic node should be compatible
        // as long as new nodes are added in compatible fashion. 

        for (Entry<String, ConfigNode> e : polymorphicNodes.entrySet()) {
            errors.push(e.getKey());
            validateChildren(singleNode, e.getValue(), context, errors);
            errors.pop();
        }
    }

    private static void validatePolymorphicToSingle(
            ConfigNode original,
            Errors errors,
            ComparisonContext comparisonContext,
            String childName,
            Node originalChild,
            Node updatedChild
    ) {
        Map<String, ConfigNode> polymorphicNode = originalChild.nodes();

        // Converting a polymorphic node with 1 subclass can be compatible
        if (polymorphicNode.size() == 2) {
            // Get a subclass
            Map.Entry<String, ConfigNode> subclass = polymorphicNode.entrySet().stream()
                    .filter(e -> !e.getKey().isEmpty())
                    .findFirst()
                    .orElseThrow();

            // Validate whether a subclass is compatible with a single node
            validateConfigNode(subclass.getKey(), subclass.getValue(), updatedChild.node(), comparisonContext, errors);
        } else {
            // Converting multiple subclasses to single node is not compatible
            errors.addChildError(original, childName, "Node was changed from polymorphic-node to single-node");
        }
    }

    private static void validatePolymorphicToPolymorphic(
            ConfigNode original,
            ComparisonContext context,
            Errors errors,
            String childName,
            Node originalChild,
            Node updatedChild
    ) {
        Map<String, ConfigNode> originalPolyNodes = originalChild.nodes();
        Map<String, ConfigNode> updatedPolyNodes = updatedChild.nodes();

        // All sub-fields per each sub-class : Original
        Map<String, Set<String>> originalNodesChildren = new LinkedHashMap<>();
        for (Entry<String, ConfigNode> e : originalPolyNodes.entrySet()) {
            originalNodesChildren.computeIfAbsent(e.getKey(), k -> new HashSet<>()).addAll(e.getValue().children().keySet());
        }

        // All sub-fields per each sub-class : Updated
        Map<String, Set<String>> updatedNodesChildren = new LinkedHashMap<>();
        for (Entry<String, ConfigNode> e : updatedPolyNodes.entrySet()) {
            updatedNodesChildren.computeIfAbsent(e.getKey(), k -> new HashSet<>()).addAll(e.getValue().children().keySet());
        }

        // No child properties has neither been added nor removed - check compatibility of every node
        if (originalNodesChildren.equals(updatedNodesChildren)) {
            for (String key : originalNodesChildren.keySet()) {
                ConfigNode originalVariant = originalPolyNodes.get(key);
                ConfigNode updatedVariant = updatedPolyNodes.get(key);

                validateConfigNode(key, originalVariant, updatedVariant, context, errors);
            }
        } else {
            // Build a map of changes per subclass
            Map<String, List<ChildChange>> changes = findChangesBetweenNodesChildren(updatedNodesChildren, originalNodesChildren);

            Set<String> skipErrorKeys = new HashSet<>();

            // Valid polymorphic-node -> polymorphic-node transformations:
            // 1. Moving a child node from a base class to all subclasses iff the child node has a default value
            // 2. Moving a child node from a subclass to a base class iff the child node has a default value.
            {
                Set<String> existedInAllSubclasses = new HashSet<>();
                for (List<ChildChange> change : changes.values()) {
                    for (ChildChange c : change) {
                        if (!c.baseClass && c.type == 0) {
                            existedInAllSubclasses.add(c.name);
                        }
                    }
                }
                for (Entry<String, Set<String>> e : originalNodesChildren.entrySet()) {
                    if (e.getKey().isEmpty()) {
                        continue;
                    }
                    existedInAllSubclasses.retainAll(e.getValue());
                }

                Set<ChildChange> movedToOrMovedFromBaseClass = changes.get(Node.BASE_INSTANCE_TYPE).stream()
                        .filter(c -> c.baseClass && (c.type == 1 || c.type == -1) && existedInAllSubclasses.contains(c.name))
                        .collect(Collectors.toSet());

                for (ChildChange c : movedToOrMovedFromBaseClass) {
                    ConfigNode originalNode = originalPolyNodes.get(c.instanceType);
                    ConfigNode updatedNode = updatedPolyNodes.get(c.instanceType);

                    Node n1 = originalNode.children().get(c.name);
                    Node n2 = updatedNode.children().get(c.name);
                    //  1 : Added: use updated node because the node has been moved from the subclass
                    // -1 : Removed: use original node because the has been moved from the base class
                    Node n = c.type == 1 ? n2 : n1;

                    // Skip removed node / added non-default
                    String onErrorKey = original.path() + "." + childName + "." + c.name;
                    skipErrorKeys.add(onErrorKey);

                    boolean singleSubclass = updatedNodesChildren.size() == 2 && originalNodesChildren.size() == 2;

                    if (!n.hasDefault() && !singleSubclass) {
                        errors.addChildError(originalNode, c.name, "Cannot move node to base class / from base class");
                    }
                }
            }

            context.skipAddRemoveKeys.addAll(skipErrorKeys);

            // Check common polymorphic nodes for incompatible changes
            for (Entry<String, ConfigNode> entry : originalPolyNodes.entrySet()) {
                String polyNodeName = entry.getKey();

                if (updatedPolyNodes.containsKey(polyNodeName)) {
                    ConfigNode originalPolyNode = entry.getValue();
                    ConfigNode updatedPolyNode = updatedPolyNodes.get(polyNodeName);

                    validateConfigNode(entry.getKey(), originalPolyNode, updatedPolyNode, context, errors);
                }
                // Adding additional polymorphic configurations is OK
            }

            // Check for removed subclasses nodes
            for (String polyNodeName : originalPolyNodes.keySet()) {
                if (!updatedPolyNodes.containsKey(polyNodeName)) {
                    errors.addChildError(original, childName, format("Polymorphic instance '{}' was removed", polyNodeName));
                }
            }

            context.skipAddRemoveKeys.removeAll(skipErrorKeys);
        }
    }

    private static Map<String, List<ChildChange>> findChangesBetweenNodesChildren(
            Map<String, Set<String>> updatedNodesChildren,
            Map<String, Set<String>> originalNodesChildren
    ) {
        Map<String, List<ChildChange>> changes = new LinkedHashMap<>();

        for (Entry<String, Set<String>> u : updatedNodesChildren.entrySet()) {
            String key = u.getKey();
            Set<String> o = originalNodesChildren.get(key);

            if (o == null) {
                List<ChildChange> changeList = new ArrayList<>();
                for (String r : u.getValue()) {
                    changeList.add(new ChildChange(key, r, 1));
                }
                changes.put(key, changeList);
            } else {
                Set<String> removed = CollectionUtils.difference(o, u.getValue());
                Set<String> remained = CollectionUtils.intersect(o, u.getValue());
                Set<String> added = CollectionUtils.difference(u.getValue(), o);

                List<ChildChange> changeList = new ArrayList<>();
                for (String r : removed) {
                    changeList.add(new ChildChange(key, r, -1));
                }
                for (String r : remained) {
                    changeList.add(new ChildChange(key, r, 0));
                }
                for (String r : added) {
                    changeList.add(new ChildChange(key, r, 1));
                }
                changes.put(key, changeList);
            }
        }

        return changes;
    }

    private static class Errors {

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

        void addError(ConfigNode node, String error) {
            reportError(node.path(), error);
        }

        void addChildError(ConfigNode node, String childName, String error) {
            reportError(node.path() + "." + childName, error);
        }

        private void reportError(String path, String error) {
            Optional<String> opt = instanceTypes.peekLast();
            String instanceType = opt.isPresent() ? opt.get() : null;

            if (instanceType != null) {
                errors.add(format("Node: {} [instanceType={}]: {}", path, instanceType, error));
            } else {
                errors.add(format("Node: {}: {}", path, error));
            }
        }

        private void throwIfNotEmpty() {
            if (!errors.isEmpty()) {
                StringBuilder message = new StringBuilder("There are incompatible changes:").append(System.lineSeparator());
                for (String error : errors) {
                    message.append('\t').append(error).append(System.lineSeparator());
                }

                throw new IllegalStateException(message.toString());
            }
        }
    }

    private static final class ChildChange {
        private final boolean baseClass;
        private final String instanceType;
        private final String name;
        private final int type;

        private ChildChange(String instanceType, String name, int type) {
            this.baseClass = instanceType.isEmpty();
            this.instanceType = instanceType;
            this.name = name;
            this.type = type;
        }

        @Override
        public String toString() {
            String inst = "[" + instanceType + "] ";
            if (type == 0) {
                return inst + name + ":" + 0;
            } else {
                return inst + name + ":" + (type > 0 ? "+1" : "-1");
            }
        }
    }
}
