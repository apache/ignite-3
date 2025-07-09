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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.KeyIgnorer;

/**
 * Compares two configuration trees (snapshot and current).
 */
public class ConfigurationTreeComparator {
    /**
     * Validates the current configuration is compatible with the snapshot.
     */
    public static void ensureCompatible(
            List<ConfigNode> snapshotTrees,
            List<ConfigNode> actualTrees,
            ComparisonContext compContext
    ) {
        LeafNodesVisitor shuttle = new LeafNodesVisitor(new Validator(actualTrees, compContext));

        for (ConfigNode tree : snapshotTrees) {
            tree.accept(shuttle);
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
     * Traverses the tree and triggers validation for leaf nodes.
     */
    private static class LeafNodesVisitor implements ConfigShuttle {
        private final Consumer<ConfigNode> validator;

        private LeafNodesVisitor(Consumer<ConfigNode> validator) {
            this.validator = validator;
        }

        @Override
        public void visit(ConfigNode node) {
            if (node.isValue()) {
                validator.accept(node);
            }
        }
    }

    /**
     * Validates value nodes.
     */
    private static class Validator implements Consumer<ConfigNode> {
        private final List<ConfigNode> roots;
        private final ComparisonContext compContext;
        private Collection<KeyIgnorer> deletedItems;

        private static final ConfigNode NO_OP_NODE = new ConfigNode();

        private Validator(List<ConfigNode> roots, ComparisonContext compContext) {
            this.roots = roots;
            this.compContext = compContext;
        }

        @Override
        public void accept(ConfigNode leafNode) {
            List<ConfigNode> path = getPath(leafNode);

            // Validate path starting from the root.
            Collection<ConfigNode> candidates = roots;

            for (ConfigNode node : path) {
                ConfigNode found = find(node, candidates);
                candidates = found.childNodes();
            }
        }

        /**
         * Return first node from candidates collection that matches the given node.
         *
         * @throws IllegalStateException If no match found.
         */
        private ConfigNode find(ConfigNode node, Collection<ConfigNode> candidates) {
            for (ConfigNode candidate : candidates) {
                if (match(node, candidate)) {
                    return candidate;
                }
            }

            if (deletedItems == null) {
                deletedItems = new ArrayList<>(compContext.configurationModules().size());

                for (ConfigurationModule module : compContext.configurationModules()) {
                    KeyIgnorer keyIgnorer = KeyIgnorer.fromDeletedPrefixes(module.deletedPrefixes());

                    deletedItems.add(keyIgnorer);
                }
            }

            boolean deleted = deletedItems.stream().anyMatch(i -> i.shouldIgnore(node.path()));

            if (deleted) {
                return NO_OP_NODE;
            }

            throw new IllegalStateException("No match found for node: " + node + " in candidates: \n\t"
                    + candidates.stream().map(ConfigNode::toString).collect(Collectors.joining("\n\t")));
        }
    }

    /**
     * Builds value node path.
     */
    private static List<ConfigNode> getPath(ConfigNode node) {
        List<ConfigNode> path = new ArrayList<>();
        while (node != null) {
            path.add(node);
            node = node.getParent();
        }

        Collections.reverse(path);

        return path;
    }

    /**
     * Returns {@code true} if given node is compatible with candidate node, {@code false} otherwise.
     */
    private static boolean match(ConfigNode node, ConfigNode candidate) {
        return Objects.equals(candidate.kind(), node.kind())
                && matchNames(candidate, node)
                && validateFlags(candidate, node)
                && candidate.deletedPrefixes().containsAll(node.deletedPrefixes())
                && (!node.isValue() || Objects.equals(candidate.type(), node.type())) // Value node types can be changed.
                // TODO https://issues.apache.org/jira/browse/IGNITE-25747 Validate annotations properly.
                && candidate.annotations().containsAll(node.annotations()); // Annotations can't be removed.
    }

    private static boolean matchNames(ConfigNode candidate, ConfigNode node) {
        return Objects.equals(candidate.name(), node.name())
                || (node.isValue() && candidate.isValue() && compareUsingLegacyNames(candidate, node));
    }

    private static boolean compareUsingLegacyNames(ConfigNode candidate, ConfigNode node) {
        return candidate.legacyPropertyNames().contains(node.name());
    }

    private static boolean validateFlags(ConfigNode candidate, ConfigNode node) {
        return node.isRoot() == candidate.isRoot()
                && node.isValue() == candidate.isValue()
                && (!candidate.isInternal() || node.isInternal()) // Public property\tree can't be hidden.
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
        private final Set<ConfigurationModule> configurationModules;

        ComparisonContext() {
            this.configurationModules = Set.of();
        }

        public ComparisonContext(Set<ConfigurationModule> configurationModules) {
            this.configurationModules = configurationModules;
        }

        Set<ConfigurationModule> configurationModules() {
            return configurationModules;
        }
    }
}
