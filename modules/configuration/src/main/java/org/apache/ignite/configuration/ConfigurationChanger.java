/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.configuration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.storage.ConfigurationStorage;
import org.apache.ignite.configuration.storage.Data;
import org.apache.ignite.configuration.storage.StorageException;
import org.apache.ignite.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.configuration.tree.InnerNode;
import org.apache.ignite.configuration.tree.NamedListNode;
import org.apache.ignite.configuration.tree.TraversableTreeNode;
import org.apache.ignite.configuration.util.ConfigurationUtil;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.configuration.validation.ValidationIssue;

// TODO: stupid stub name, think later
public class ConfigurationChanger {
    /** Map of configurations' configurators. */
    private Map<RootKey<?>, Configurator<?>> registry = new HashMap<>();

    /** Storage. */
    private ConfigurationStorage configurationStorage;

    /** Changer's last known version of storage. */
    private final AtomicInteger version = new AtomicInteger(0);

    /** Constructor. */
    public ConfigurationChanger(ConfigurationStorage configurationStorage) {
        this.configurationStorage = configurationStorage;
    }

    /**
     * Initialize changer.
     */
    public void init() throws ConfigurationChangeException {
        final Data data;

        try {
            data = configurationStorage.readAll();
        }
        catch (StorageException e) {
            throw new ConfigurationChangeException("Failed to initialize configuration: " + e.getMessage(), e);
        }

        version.set(data.version());

        configurationStorage.addListener(this::updateFromListener);

        // TODO: IGNITE-14118 iterate over data and fill Configurators
    }

    /**
     * Add configurator.
     * @param key Root configuration key of the configurator.
     * @param configurator Configuration's configurator.
     */
    public void registerConfiguration(RootKey<?> key, Configurator<?> configurator) {
        registry.put(key, configurator);
    }

    /**
     * Change configuration.
     * @param changes Map of changes by root key.
     */
    public void change(Map<RootKey<?>, TraversableTreeNode> changes) throws ConfigurationChangeException,
        ConfigurationValidationException {
        Map<String, Serializable> allChanges = changes.entrySet().stream()
            .map((Map.Entry<RootKey<?>, TraversableTreeNode> change) -> convertChangesToMap(change.getKey(), change.getValue()))
            .flatMap(map -> map.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        boolean success = false;

        List<ValidationIssue> validationIssues = Collections.emptyList();

        while (!success) {
            final ValidationResult validationResult = validate(changes);

            validationIssues = validationResult.issues();

            final int version = validationResult.version();

            if (validationIssues.isEmpty())
                try {
                    success = configurationStorage.write(allChanges, version);
                }
                catch (StorageException e) {
                    throw new ConfigurationChangeException("Failed to change configuration: " + e.getMessage(), e);
                }
            else
                break;
        }

        if (!validationIssues.isEmpty())
            throw new ConfigurationValidationException(validationIssues);
    }

    private synchronized void updateFromListener(Data changedEntries) {
        // TODO: IGNITE-14118 add tree update
        version.set(changedEntries.version());
    }

    private synchronized ValidationResult validate(Map<RootKey<?>, TraversableTreeNode> changes) {
        final int version = this.version.get();

        List<ValidationIssue> issues = new ArrayList<>();

        for (Map.Entry<RootKey<?>, TraversableTreeNode> entry : changes.entrySet()) {
            RootKey<?> rootKey = entry.getKey();
            TraversableTreeNode changesForRoot = entry.getValue();

            final Configurator<?> configurator = registry.get(rootKey);

            List<ValidationIssue> list = configurator.validateChanges(changesForRoot);
            issues.addAll(list);
        }

        return new ValidationResult(issues, version);
    }

    /**
     * Convert a traversable tree to a map of qualified keys to values.
     * @param rootKey Root configuration key.
     * @param node Tree.
     * @return Map of changes.
     */
    private Map<String, Serializable> convertChangesToMap(RootKey<?> rootKey, TraversableTreeNode node) {
        Map<String, Serializable> values = new HashMap<>();

        node.accept(null, new ConfigurationVisitor() {
            /** Current key, aggregated by visitor. */
            StringBuilder currentKey = new StringBuilder(rootKey.key());

            /** {@inheritDoc} */
            @Override public void visitLeafNode(String key, Serializable val) {
                values.put(currentKey.toString() + "." + key, val);
            }

            /** {@inheritDoc} */
            @Override public void visitInnerNode(String key, InnerNode node) {
                if (node == null)
                    return;

                String previousKey = currentKey.toString();

                if (key != null)
                    currentKey.append('.').append(key);

                node.traverseChildren(this);

                currentKey = new StringBuilder(previousKey);
            }

            /** {@inheritDoc} */
            @Override public <N extends InnerNode> void visitNamedListNode(String key, NamedListNode<N> node) {
                String previousKey = currentKey.toString();

                if (key != null)
                    currentKey.append('.').append(key);

                for (String namedListKey : node.namedListKeys()) {
                    String loopPreviousKey = currentKey.toString();
                    currentKey.append('.').append(ConfigurationUtil.escape(namedListKey));

                    node.get(namedListKey).traverseChildren(this);

                    currentKey = new StringBuilder(loopPreviousKey);
                }

                currentKey = new StringBuilder(previousKey);
            }
        });
        return values;
    }

    private static final class ValidationResult {
        private final List<ValidationIssue> issues;
        private final int version;

        public ValidationResult(List<ValidationIssue> issues, int version) {
            this.issues = issues;
            this.version = version;
        }

        public List<ValidationIssue> issues() {
            return issues;
        }

        public int version() {
            return version;
        }
    }
}
