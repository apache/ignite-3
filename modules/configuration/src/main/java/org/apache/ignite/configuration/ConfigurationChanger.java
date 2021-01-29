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
import org.apache.ignite.configuration.tree.ConfigurationVisitor;
import org.apache.ignite.configuration.tree.InnerNode;
import org.apache.ignite.configuration.tree.NamedListNode;
import org.apache.ignite.configuration.tree.TraversableTreeNode;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.configuration.validation.ValidationIssue;

// TODO: stupid stub name, think later
public class ConfigurationChanger {

    private Map<RootKey<?>, Configurator<?>> registry = new HashMap<>();

    private ConfigurationStorage configurationStorage;

    private final AtomicInteger version = new AtomicInteger(0);

    public ConfigurationChanger(ConfigurationStorage configurationStorage) {
        this.configurationStorage = configurationStorage;
    }

    public void init() {
        final Data data = configurationStorage.readAll();
        version.set(data.version());

        configurationStorage.addListener(changedEntries -> {
            // TODO: add tree update
            version.set(changedEntries.version());
        });

        // TODO: iterate over data and fill Configurators
    }

    public void registerConfiguration(RootKey<?> key, Configurator<?> configurator) {
        registry.put(key, configurator);
    }

    public <T extends ConfigurationTree<?, ?>> void change(Map<RootKey<?>, TraversableTreeNode> changes) {
        Map<String, Serializable> allChanges = changes.entrySet().stream()
            .map((Map.Entry<RootKey<?>, TraversableTreeNode> change) -> convertChangesToMap(change.getKey(), change.getValue()))
            .flatMap(map -> map.entrySet().stream())
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        boolean success = false;

        List<ValidationIssue> validationIssues = Collections.emptyList();

        while (!success) {
            validationIssues = validate(changes);

            final int version = this.version.get();

            if (validationIssues.isEmpty())
                success = configurationStorage.write(allChanges, version);
            else
                break;
        }

        if (!validationIssues.isEmpty())
            throw new ConfigurationValidationException(validationIssues);
    }

    private List<ValidationIssue> validate(Map<RootKey<?>, TraversableTreeNode> changes) {
        List<ValidationIssue> issues = new ArrayList<>();

        for (Map.Entry<RootKey<?>, TraversableTreeNode> entry : changes.entrySet()) {
            RootKey<?> rootKey = entry.getKey();
            TraversableTreeNode changesForRoot = entry.getValue();

            final Configurator<?> configurator = registry.get(rootKey);

            List<ValidationIssue> list = configurator.validateChanges(changesForRoot);
            issues.addAll(list);
        }

        return issues;
    }

    private Map<String, Serializable> convertChangesToMap(RootKey<?> rootKey, TraversableTreeNode node) {
        Map<String, Serializable> values = new HashMap<>();

        node.accept(null, new ConfigurationVisitor() {

            StringBuilder currentKey = new StringBuilder(rootKey.key());

            @Override public void visitLeafNode(String key, Serializable val) {
                values.put(currentKey.toString() + "." + key, val);
            }

            @Override public void visitInnerNode(String key, InnerNode node) {
                String previousKey = currentKey.toString();

                if (key != null)
                    currentKey.append('.').append(key);

                node.traverseChildren(this);

                currentKey = new StringBuilder(previousKey);
            }

            @Override public <N extends InnerNode> void visitNamedListNode(String key, NamedListNode<N> node) {
                String previousKey = currentKey.toString();

                if (key != null)
                    currentKey.append('.').append(key);

                for (String namedListKey : node.namedListKeys()) {
                    String loopPreviousKey = currentKey.toString();
                    currentKey.append('.').append(namedListKey);

                    node.get(namedListKey).traverseChildren(this);

                    currentKey = new StringBuilder(loopPreviousKey);
                }

                currentKey = new StringBuilder(previousKey);
            }
        });
        return values;
    }

}
