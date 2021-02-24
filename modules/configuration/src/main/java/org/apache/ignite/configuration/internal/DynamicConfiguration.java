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

package org.apache.ignite.configuration.internal;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.ConfigurationChanger;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.ConfigurationTree;
import org.apache.ignite.configuration.RootKey;
import org.apache.ignite.configuration.tree.ConfigurationSource;
import org.apache.ignite.configuration.tree.ConstructableTreeNode;
import org.apache.ignite.configuration.tree.InnerNode;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.configuration.validation.FieldValidator;

import static org.apache.ignite.configuration.internal.util.ConfigurationUtil.fillFromPrefixMap;
import static org.apache.ignite.configuration.internal.util.ConfigurationUtil.find;
import static org.apache.ignite.configuration.internal.util.ConfigurationUtil.join;
import static org.apache.ignite.configuration.internal.util.ConfigurationUtil.toPrefixMap;

/**
 * This class represents configuration root or node.
 */
public abstract class DynamicConfiguration<VIEW, INIT, CHANGE> extends ConfigurationNode<VIEW> implements Modifier<VIEW, INIT, CHANGE>, ConfigurationTree<VIEW, CHANGE> {

    /** Configuration members (leaves and nodes). */
    protected final Map<String, Modifier<?, ?, ?>> members = new HashMap<>();

    /**
     * Constructor.
     * @param prefix Configuration prefix.
     * @param key Configuration key.
     */
    protected DynamicConfiguration(
        List<String> prefix,
        String key,
        RootKey<?> rootKey,
        ConfigurationChanger changer
    ) {
        super(prefix, key, rootKey, changer);
    }

    /**
     * Add new configuration member.
     * @param member Configuration member (leaf or node).
     * @param <M> Type of member.
     */
    protected <M extends Modifier<?, ?, ?>> void add(M member) {
        members.put(member.key(), member);
    }

    /**
     * Add new configuration member with validators.
     * @param member Configuration member (leaf or node).
     * @param validators Validators for new member.
     * @param <PROP> Type of {@link DynamicProperty}.
     * @param <M> Type of member.
     */
    protected <PROP extends Serializable, M extends DynamicProperty<PROP>> void add(
        M member,
        List<FieldValidator<? super PROP, ? extends ConfigurationTree<?, ?>>> validators
    ) {
        members.put(member.key(), member);

//        configurator.addValidations((Class<? extends ConfigurationTree<?, ?>>) getClass(), member.key(), validators);
    }

    /** {@inheritDoc} */
    @Override public Future<Void> change(Consumer<CHANGE> change) throws ConfigurationValidationException {
        Objects.requireNonNull(change, "Configuration consumer cannot be null.");

        InnerNode rootNodeChange = ((RootKeyImpl)rootKey).createRootNode();

        if (keys.size() == 1)
            change.accept((CHANGE)rootNodeChange);
        else {
            // TODO Not optimal, can be improved. Do it when tests are ready.
            fillFromPrefixMap(rootNodeChange, toPrefixMap(Collections.singletonMap(join(keys), null)));

            ConstructableTreeNode parent = (ConstructableTreeNode)find(keys.subList(0, keys.size() - 1), rootNodeChange);

            parent.construct(key, new ConfigurationSource() {
                @Override public void descend(ConstructableTreeNode node) {
                    change.accept((CHANGE)node);
                }
            });
        }

        return changer.change(Map.of(rootKey, rootNodeChange));
    }

    /** {@inheritDoc} */
    @Override public String key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public VIEW value() {
        return viewValue();
    }

    /** {@inheritDoc} */
    @Override public Map<String, ConfigurationProperty<?, ?>> members() {
        return members.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /** {@inheritDoc} */
    @Override protected void refresh0(VIEW val) {
        // No-op.
    }
}
