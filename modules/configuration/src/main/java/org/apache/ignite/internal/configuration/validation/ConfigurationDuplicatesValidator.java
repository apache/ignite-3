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

package org.apache.ignite.internal.configuration.validation;

import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.impl.Parseable;
import com.typesafe.config.parser.ConfigDocument;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.jetbrains.annotations.Nullable;

/** Validates that there are no duplicate keys in the configuration. */
public class ConfigurationDuplicatesValidator {
    /**
     * Validates that there are no duplicate keys in the passed configuration.
     *
     * @param cfg configuration in HOCON or JSON format.
     */
    public static Collection<ValidationIssue> validate(String cfg) {
        Object root = getConfigRoot(cfg);

        Queue<Node> queue = new ArrayDeque<>();
        queue.add(new Node(root, new ArrayList<>(), null));

        Set<String> paths = new HashSet<>();
        Set<ValidationIssue> issues = new HashSet<>();

        while (!queue.isEmpty()) {
            Node currentNode = queue.poll();
            List<Object> nodeChildren = getChildrenOrEmpty(currentNode.configNode);

            if (nodeChildren.isEmpty()) {
                continue;
            }

            Object firstChild = nodeChildren.get(0);
            ArrayList<String> currentPath = new ArrayList<>(currentNode.basePath);

            if (ConfigNodePath.isInstance(firstChild) || currentNode.index != null) {
                currentPath.add(ConfigNodePath.isInstance(firstChild) ? ConfigNodePath.path(firstChild) : currentNode.index.toString());
                String path = String.join(".", currentPath);

                if (paths.contains(path)) {
                    issues.add(new ValidationIssue(path, "Duplicated key"));
                }

                paths.add(path);
            }

            int index = 0;
            for (Object child : nodeChildren) {
                if (ConfigNodeComplexValue.isInstance(child) || ConfigNodeFieldClass.isInstance(child)) {
                    queue.add(new Node(child, currentPath, ConfigNodeArray.isInstance(currentNode.configNode) ? index : null));
                    index++;
                }
            }
        }

        return issues;
    }

    private static Object getConfigRoot(String cfg) {
        ConfigDocument configDocument = Parseable.newString(cfg, ConfigParseOptions.defaults()).parseConfigDocument();
        assert SimpleConfigDocument.isInstance(configDocument);

        Object root = SimpleConfigDocument.root(configDocument);
        assert ConfigNodeComplexValue.isInstance(root);

        return root;
    }

    private static List<Object> getChildrenOrEmpty(Object configNode) {
        if (ConfigNodeComplexValue.isInstance(configNode)) {
            return ConfigNodeComplexValue.children(configNode);
        } else if (ConfigNodeFieldClass.isInstance(configNode)) {
            return ConfigNodeFieldClass.children(configNode);
        } else {
            return List.of();
        }
    }

    private static class ConfigNodeComplexValue {
        private static final Class<?> CLASS;
        private static final Field CHILDREN_FIELD;

        static {
            try {
                CLASS = Class.forName("com.typesafe.config.impl.ConfigNodeComplexValue");

                CHILDREN_FIELD = CLASS.getDeclaredField("children");
                CHILDREN_FIELD.setAccessible(true);
            } catch (Exception e) {
                // Shouldn't happen unless library structure is changed
                throw new RuntimeException(e);
            }
        }

        private static boolean isInstance(Object configNode) {
            return CLASS.isInstance(configNode);
        }

        private static List<Object> children(Object configNodeComplexValue) {
            try {
                return (List<Object>) CHILDREN_FIELD.get(configNodeComplexValue);
            } catch (IllegalAccessException e) {
                // Shouldn't happen
                throw new RuntimeException(e);
            }
        }
    }

    private static class ConfigNodeArray {
        private static final Class<?> CLASS;

        static {
            try {
                CLASS = Class.forName("com.typesafe.config.impl.ConfigNodeArray");
            } catch (Exception e) {
                // Shouldn't happen unless library structure is changed
                throw new RuntimeException(e);
            }
        }

        private static boolean isInstance(Object configNode) {
            return CLASS.isInstance(configNode);
        }
    }

    private static class ConfigNodeFieldClass {
        private static final Class<?> CLASS;
        private static final Field CHILDREN_FIELD;

        static {
            try {
                CLASS = Class.forName("com.typesafe.config.impl.ConfigNodeField");

                CHILDREN_FIELD = CLASS.getDeclaredField("children");
                CHILDREN_FIELD.setAccessible(true);
            } catch (Exception e) {
                // Shouldn't happen unless library structure is changed
                throw new RuntimeException(e);
            }
        }

        private static boolean isInstance(Object configNode) {
            return CLASS.isInstance(configNode);
        }

        private static List<Object> children(Object configNode) {
            try {
                return (List<Object>) CHILDREN_FIELD.get(configNode);
            } catch (IllegalAccessException e) {
                // Shouldn't happen
                throw new RuntimeException(e);
            }
        }
    }

    private static class SimpleConfigDocument {
        private static final Class<?> CLASS;
        private static final Field NODE_TREE_FIELD;

        static {
            try {
                CLASS = Class.forName("com.typesafe.config.impl.SimpleConfigDocument");

                NODE_TREE_FIELD = CLASS.getDeclaredField("configNodeTree");
                NODE_TREE_FIELD.setAccessible(true);
            } catch (Exception e) {
                // Shouldn't happen unless library structure is changed
                throw new RuntimeException(e);
            }
        }

        private static boolean isInstance(Object configDocument) {
            return CLASS.isInstance(configDocument);
        }

        private static Object root(Object simpleConfigDocument) {
            try {
                return NODE_TREE_FIELD.get(simpleConfigDocument);
            } catch (IllegalAccessException e) {
                // Shouldn't happen
                throw new RuntimeException(e);
            }
        }
    }

    private static class ConfigNodePath {
        private static final Class<?> CLASS;
        private static final Class<?> PATH_CLASS;
        private static final Field PATH_FIELD;
        private static final Method PATH_RENDER_METHOD;

        static {
            try {
                CLASS = Class.forName("com.typesafe.config.impl.ConfigNodePath");
                PATH_CLASS = Class.forName("com.typesafe.config.impl.Path");

                PATH_FIELD = CLASS.getDeclaredField("path");
                PATH_FIELD.setAccessible(true);

                PATH_RENDER_METHOD = PATH_CLASS.getDeclaredMethod("render");
                PATH_RENDER_METHOD.setAccessible(true);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        private static boolean isInstance(Object configNode) {
            return CLASS.isInstance(configNode);
        }

        private static String path(Object firstChild) {
            try {
                Object path = PATH_FIELD.get(firstChild);

                return PATH_RENDER_METHOD.invoke(path).toString();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class Node {
        private final List<String> basePath;
        private final Object configNode;
        private final @Nullable Integer index;

        private Node(Object configNode, List<String> basePath, @Nullable Integer index) {
            this.configNode = configNode;
            this.basePath = basePath;
            this.index = index;
        }
    }
}
