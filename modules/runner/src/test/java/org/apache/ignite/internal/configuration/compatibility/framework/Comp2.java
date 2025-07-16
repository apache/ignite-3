package org.apache.ignite.internal.configuration.compatibility.framework;

import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.ignite.internal.configuration.compatibility.GenerateConfigurationSnapshot;
import org.apache.ignite.internal.configuration.compatibility.framework.ConfigurationTreeComparator.ComparisonContext;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.util.CollectionUtils;

public class Comp2 {

    private static final IgniteLogger LOG = Loggers.forClass(GenerateConfigurationSnapshot.class);

    private final ComparisonContext comparisonContext;

    public Comp2() {
        this.comparisonContext = new ComparisonContext();
    }

    public Comp2(ComparisonContext comparisonContext) {
        this.comparisonContext = comparisonContext;
    }

    public void ensureCompatible(List<ConfigNode> previousRoots, List<ConfigNode> currentRoots) {
        Map<String, Map<String, ConfigNode>> previousRootsByKind = groupByKind(previousRoots);
        Map<String, Map<String, ConfigNode>> currentRootsByKind = groupByKind(currentRoots);

        // Compare configuration kinds
        if (!previousRootsByKind.keySet().equals(currentRootsByKind.keySet())) {
            String error = format("Configuration kind do not match. Expected: {} but got {}",
                    previousRootsByKind.keySet(),
                    currentRootsByKind.keySet()
            );

            throw new IllegalStateException(error);
        }

        // Then compare roots one by one
        for (Entry<String, Map<String, ConfigNode>> entry : previousRootsByKind.entrySet()) {
            Map<String, ConfigNode> prev = entry.getValue();
            Map<String, ConfigNode> current = currentRootsByKind.get(entry.getKey());

            compareConfigRoots(prev, current);
        }
    }

    private void compareConfigRoots(Map<String, ConfigNode> previousRoots, Map<String, ConfigNode> currentRoots) {
        Set<String> removed = CollectionUtils.difference(previousRoots.keySet(), currentRoots.keySet());

        // Check config roots
        if (!removed.isEmpty()) {
            String error = format("Incompatible change. Some of the root keys has been removed.\n"
                            + "Removed root keys: {}\n"
                            + "Previous root keys: {}\n"
                            + "Current root keys: {}\n",
                    removed, previousRoots.keySet(), currentRoots.keySet());

            throw new IllegalStateException(error);
        }

        for (Map.Entry<String, ConfigNode> e : previousRoots.entrySet()) {
            ConfigNode current = currentRoots.get(e.getKey());
            if (current == null) {
                // adding a configuration root is a compatible change.
                continue;
            }

            ConfigNode previous = e.getValue();
            ensureCompareCompatible(previous, current);
        }
    }

    public void ensureCompareCompatible(ConfigNode previous, ConfigNode current) {
        // List of paths to support polymorphic nodes.
        // Converts configuration trees to a map of paths
        Map<String, List<Path>> previousNodes = buildPaths(previous);
        Map<String, List<Path>> currentNodes = buildPaths(current);

        if (LOG.isInfoEnabled()) {
            LOG.info("Previous: {}", pathsToString(previousNodes));
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("Current: {}", pathsToString(previousNodes));
        }

        Set<String> removed = CollectionUtils.difference(previousNodes.keySet(), currentNodes.keySet());
        Set<String> added = CollectionUtils.difference(currentNodes.keySet(), previousNodes.keySet());
        Set<String> unchanged = CollectionUtils.intersect(previousNodes.keySet(), currentNodes.keySet());

        List<String> errors = new ArrayList<>();

        validateRemovals(removed, comparisonContext, errors);
        validateAdded(currentNodes, added, errors);
        validateRemaining(previousNodes, currentNodes, unchanged, errors);

        reportErrors(errors);
    }

    private static void validateRemovals(Set<String> keys, ComparisonContext comparisonContext, List<String> errors) {
        LOG.info("Removed keys: {}", keys);

        Set<String> removedFiltered = keys.stream().filter(r -> !comparisonContext.shouldIgnore(r)).collect(Collectors.toSet());
        if (!removedFiltered.isEmpty()) {
            errors.add("These properties has been removed: " + keys);
        }
    }

    private static void validateAdded(Map<String, List<Path>> currentNodes, Set<String> keys, List<String> errors) {
        LOG.info("Added keys: {}", keys);

        Set<String> nonDefaults = new HashSet<>();

        for (String key : keys) {
            List<Path> addedPaths = currentNodes.get(key);

            for (Path path : addedPaths) {
                for (ConfigNode node : path.nodes) {
                    if (node.isValue() && !node.hasDefault()) {
                        nonDefaults.add(node.path());
                    }
                }
            }
        }

        if (!nonDefaults.isEmpty()) {
            errors.add("Adding properties without default values is incompatible change: " + nonDefaults);
        }
    }

    private static void validateRemaining(
            Map<String, List<Path>> previousNodes,
            Map<String, List<Path>> currentNodes,
            Set<String> keys,
            List<String> errors
    ) {

        LOG.info("Remained keys: {}", keys);

        for (String key : keys) {
            List<Path> previous = previousNodes.get(key);
            List<Path> current = currentNodes.get(key);

            for (int i = 0; i < previous.size(); i++) {
                validatePath(previous.get(i), current.get(i), errors);
            }
        }

        reportErrors(errors);
    }

    private static void validatePath(Path previousPath, Path currentPath, List<String> errors) {
        for (int i = 0; i < previousPath.nodes.size(); i++) {
            ConfigNode previous = previousPath.nodes.get(i);
            ConfigNode current = currentPath.nodes.get(i);

            boolean match = ConfigurationTreeComparator.match(previous, current, null);
            if (!match) {
                errors.add("Node has incompatible changes: " + current.path());
            }
        }
    }

    private static void reportErrors(List<String> errors) {
        if (!errors.isEmpty()) {
            StringBuilder sb = new StringBuilder("Incompatible changes").append(System.lineSeparator());
            for (String message : errors) {
                sb.append('\t').append(message).append(System.lineSeparator());
            }

            throw new IllegalStateException(sb.toString());
        }
    }

    private static Map<String, Map<String, ConfigNode>> groupByKind(List<ConfigNode> nodes) {
        Map<String, Map<String, ConfigNode>> out = new LinkedHashMap<>();

        for (ConfigNode node : nodes) {
            Map<String, ConfigNode> byKind = out.computeIfAbsent(node.kind(), (k) -> new LinkedHashMap<>());
            ConfigNode prev = byKind.put(node.name(), node);
            assert prev == null : "Duplicate name";
        }

        return out;
    }

    private static Map<String, List<Path>> buildPaths(ConfigNode node) {
        Map<String, List<Path>> paths = new LinkedHashMap<>();

        collect(node, new CurrentPath(), paths);

        return paths;
    }

    /**
     * Converts a configuration tree into lists of paths from a root to leafs.
     * 
     * <pre>
     *    A
     *  /  \
     * B    C
     *     / \  
     *    D   E
     * </pre>
     * 
     * Gets converted to:
     * <pre>
     *     {A.B: [A, B], A.C.D: [A, C, D], A.C.E: {A, C, E}}
     * </pre>
     */
    private static void collect(ConfigNode node, CurrentPath path, Map<String, List<Path>> paths) {
        if (node.isValue()) {
            CurrentPath end = path.newChild(node.name(), node);
            String key = String.join(".", end.keys);

            List<Path> out = paths.computeIfAbsent(key, (k) -> new ArrayList<>());
            out.add(new Path(end.nodes));
        } else {
            CurrentPath subPath = path.newChild(node.name(), node);

            for (var ref : node.childNodes()) {
                List<ConfigNode> nodes = new ArrayList<>(ref.nodes());
                nodes.sort(Comparator.comparing(ConfigNode::name).thenComparing(ConfigNode::className));

                for (var child : nodes) {
                    collect(child, subPath, paths);
                }
            }
        }
    }

    private static class Path {
        final List<ConfigNode> nodes;

        private Path(List<ConfigNode> nodes) {
            this.nodes = List.copyOf(nodes);
        }

        @Override
        public String toString() {
            return "Path{" +
                    ", nodes=" + nodes.stream().map(n -> n.path() + " " + n.attributes()).collect(Collectors.toSet()) +
                    '}';
        }
    }

    private static class CurrentPath {
        private final List<String> keys = new ArrayList<>();
        private final List<ConfigNode> nodes = new ArrayList<>();

        private CurrentPath() {

        }

        CurrentPath(CurrentPath src) {
            this.keys.addAll(src.keys);
            this.nodes.addAll(src.nodes);
        }

        CurrentPath newChild(String name, ConfigNode node) {
            CurrentPath path = new CurrentPath(this);
            path.keys.add(name);
            path.nodes.add(node);
            return path;
        }
    }

    private static String pathsToString(Map<String, List<Path>> nodes) {
        StringBuilder sb = new StringBuilder().append(System.lineSeparator());

        for (Entry<String, List<Path>> e : new TreeMap<>(nodes).entrySet()) {
            List<Path> paths = e.getValue();
            boolean hasSubclasses = paths.isEmpty();

            sb.append("\t").append(e.getKey()).append("=").append(paths.size()).append(System.lineSeparator());
            for (var p : paths) {
                sb.append("\t\t").append(pathToString(p, hasSubclasses)).append(System.lineSeparator());
            }
        }

        return sb.toString();
    }

    private static String pathToString(Path path, boolean hasSubclasses) {
        StringBuilder sb = new StringBuilder();

        for (ConfigNode node : path.nodes) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(node.name());
        }

        return sb.toString();
    }
}
